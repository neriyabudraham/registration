/**
 * One-time migration script to fix label names (remove underscores)
 * Run with: node src/migrations/fixLabels.js
 */

require('dotenv').config();
const mysql = require('mysql2/promise');
const axios = require('axios');
const EncryptionService = require('../services/encryption');

// Use the actual encryption service from the project
const encryptionService = new EncryptionService(process.env.ENCRYPTION_KEY);

function decrypt(encryptedText) {
    if (!encryptedText) return null;
    
    // Check if it looks like a raw Google token (starts with 1//)
    if (encryptedText.startsWith('1//')) {
        return encryptedText;
    }
    
    // Try to decrypt
    const decrypted = encryptionService.decrypt(encryptedText);
    return decrypted || encryptedText; // Return original if decryption fails
}

async function refreshAccessToken(refreshToken) {
    try {
        const response = await axios.post('https://oauth2.googleapis.com/token', {
            client_id: process.env.GOOGLE_CLIENT_ID,
            client_secret: process.env.GOOGLE_CLIENT_SECRET,
            refresh_token: refreshToken,
            grant_type: 'refresh_token'
        });
        return response.data.access_token;
    } catch (error) {
        console.error('Failed to refresh token:', error.message);
        return null;
    }
}

async function getLabels(accessToken) {
    try {
        const response = await axios.get(
            'https://people.googleapis.com/v1/contactGroups?pageSize=1000',
            {
                headers: { Authorization: `Bearer ${accessToken}` }
            }
        );
        return response.data.contactGroups || [];
    } catch (error) {
        console.error('Failed to get labels:', error.response?.data?.error?.message || error.message);
        return null;
    }
}

async function renameLabel(accessToken, resourceName, newName) {
    try {
        // First get the label to get etag
        const getResponse = await axios.get(
            `https://people.googleapis.com/v1/${resourceName}`,
            {
                headers: { Authorization: `Bearer ${accessToken}` }
            }
        );
        
        const etag = getResponse.data.etag;
        
        // Update the label
        await axios.put(
            `https://people.googleapis.com/v1/${resourceName}`,
            {
                contactGroup: {
                    name: newName,
                    etag: etag
                }
            },
            {
                headers: { 
                    Authorization: `Bearer ${accessToken}`,
                    'Content-Type': 'application/json'
                }
            }
        );
        
        return true;
    } catch (error) {
        console.error('Failed to rename label:', error.response?.data?.error?.message || error.message);
        return false;
    }
}

async function run() {
    console.log('🚀 Starting label fix migration...\n');
    
    const pool = mysql.createPool({
        host: process.env.MYSQL_HOST || 'localhost',
        port: process.env.MYSQL_PORT || 3306,
        user: process.env.MYSQL_USER,
        password: process.env.MYSQL_PASSWORD,
        database: process.env.MYSQL_DATABASE,
        waitForConnections: true,
        connectionLimit: 5
    });
    
    const connection = await pool.getConnection();
    
    try {
        // Step 1: Create temporary tracking table
        console.log('📋 Creating tracking table...');
        await connection.execute(`
            CREATE TABLE IF NOT EXISTS migration_label_fixes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_phone VARCHAR(20),
                customer_name VARCHAR(255),
                label_resource_name VARCHAR(255),
                old_label_name VARCHAR(255),
                new_label_name VARCHAR(255),
                status ENUM('pending', 'fixed', 'error', 'skipped') DEFAULT 'pending',
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fixed_at TIMESTAMP NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        `);
        
        // Step 2: Get all customers with valid tokens
        console.log('👥 Getting customers with valid tokens...');
        const [customers] = await connection.execute(`
            SELECT Phone, FullName, Email, AccessToken, RefreshToken
            FROM לקוחות
            WHERE AccessToken IS NOT NULL AND RefreshToken IS NOT NULL
            AND Phone IS NOT NULL
        `);
        
        console.log(`Found ${customers.length} customers with tokens\n`);
        
        let totalLabelsFound = 0;
        let totalLabelsFixed = 0;
        let totalErrors = 0;
        
        // Step 3: For each customer, find labels with underscores
        for (const customer of customers) {
            console.log(`\n🔍 Processing: ${customer.FullName || customer.Phone}`);
            
            // Decrypt tokens
            let accessToken = decrypt(customer.AccessToken);
            const refreshToken = decrypt(customer.RefreshToken);
            
            // Debug: show token info
            const tokenStart = refreshToken?.substring(0, 20) || 'null';
            const isGoogleFormat = refreshToken?.startsWith('1//');
            console.log(`  Token preview: ${tokenStart}... (Google format: ${isGoogleFormat})`);
            
            if (!refreshToken) {
                console.log(`  ⚠️ Decryption returned null - skipping`);
                continue;
            }
            
            // Try to refresh access token
            const newAccessToken = await refreshAccessToken(refreshToken);
            if (!newAccessToken) {
                console.log(`  ⚠️ Could not refresh token - skipping`);
                continue;
            }
            accessToken = newAccessToken;
            
            // Get labels
            const labels = await getLabels(accessToken);
            if (!labels) {
                console.log(`  ⚠️ Could not get labels - skipping`);
                continue;
            }
            
            // Find labels with underscores (user-created labels only)
            const labelsToFix = labels.filter(label => {
                if (label.groupType !== 'USER_CONTACT_GROUP') return false;
                return label.name && label.name.includes('_');
            });
            
            if (labelsToFix.length === 0) {
                console.log(`  ✓ No labels to fix`);
                continue;
            }
            
            console.log(`  📝 Found ${labelsToFix.length} labels to fix:`);
            
            for (const label of labelsToFix) {
                const oldName = label.name;
                const newName = oldName.replace(/_/g, ' ');
                
                console.log(`    - "${oldName}" → "${newName}"`);
                totalLabelsFound++;
                
                // Insert into tracking table
                await connection.execute(`
                    INSERT INTO migration_label_fixes 
                    (customer_phone, customer_name, label_resource_name, old_label_name, new_label_name, status)
                    VALUES (?, ?, ?, ?, ?, 'pending')
                `, [customer.Phone, customer.FullName, label.resourceName, oldName, newName]);
                
                // Try to fix the label
                const success = await renameLabel(accessToken, label.resourceName, newName);
                
                if (success) {
                    await connection.execute(`
                        UPDATE migration_label_fixes 
                        SET status = 'fixed', fixed_at = NOW()
                        WHERE customer_phone = ? AND label_resource_name = ?
                    `, [customer.Phone, label.resourceName]);
                    console.log(`      ✅ Fixed!`);
                    totalLabelsFixed++;
                } else {
                    await connection.execute(`
                        UPDATE migration_label_fixes 
                        SET status = 'error', error_message = 'Failed to rename'
                        WHERE customer_phone = ? AND label_resource_name = ?
                    `, [customer.Phone, label.resourceName]);
                    console.log(`      ❌ Error`);
                    totalErrors++;
                }
                
                // Rate limit protection
                await new Promise(r => setTimeout(r, 500));
            }
        }
        
        // Step 4: Show summary
        console.log('\n' + '='.repeat(50));
        console.log('📊 MIGRATION SUMMARY');
        console.log('='.repeat(50));
        console.log(`Total labels found with underscores: ${totalLabelsFound}`);
        console.log(`Successfully fixed: ${totalLabelsFixed}`);
        console.log(`Errors: ${totalErrors}`);
        
        // Show results from table
        const [results] = await connection.execute(`
            SELECT status, COUNT(*) as count FROM migration_label_fixes GROUP BY status
        `);
        console.log('\nResults by status:');
        results.forEach(r => console.log(`  ${r.status}: ${r.count}`));
        
        // Step 5: Ask if should delete tracking table
        console.log('\n📋 Tracking table "migration_label_fixes" contains the full log.');
        console.log('You can inspect it in MySQL, then delete with:');
        console.log('  DROP TABLE migration_label_fixes;');
        
    } catch (error) {
        console.error('\n❌ Migration error:', error.message);
    } finally {
        connection.release();
        await pool.end();
    }
    
    console.log('\n✅ Migration complete!');
}

run().catch(console.error);
