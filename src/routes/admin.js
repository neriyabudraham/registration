const express = require('express');
const router = express.Router();
const EncryptionService = require('../services/encryption');
const jwt = require('jsonwebtoken');
const axios = require('axios');

module.exports = (pool, config, contactSaver) => {
    const encryption = new EncryptionService(config.encryptionKey);
    const ADMIN_EMAIL = 'office@neriyabudraham.co.il';
    const JWT_SECRET = config.jwtSecret || 'admin-jwt-secret-change-me';

    // Middleware to verify admin token
    const verifyAdmin = (req, res, next) => {
        const token = req.cookies?.adminToken || req.headers['authorization']?.replace('Bearer ', '');
        
        if (!token) {
            return res.status(401).json({ error: 'Unauthorized' });
        }
        
        try {
            const decoded = jwt.verify(token, JWT_SECRET);
            if (decoded.email !== ADMIN_EMAIL) {
                return res.status(403).json({ error: 'Forbidden' });
            }
            req.admin = decoded;
            next();
        } catch (error) {
            return res.status(401).json({ error: 'Invalid token' });
        }
    };

    // Check if admin password exists
    router.get('/setup-status', async (req, res) => {
        const connection = await pool.getConnection();
        try {
            const [rows] = await connection.execute(
                'SELECT * FROM admin_settings WHERE setting_key = ?',
                ['admin_password']
            );
            res.json({ 
                hasPassword: rows.length > 0,
                email: ADMIN_EMAIL 
            });
        } catch (error) {
            // Table might not exist
            res.json({ hasPassword: false, email: ADMIN_EMAIL });
        } finally {
            connection.release();
        }
    });

    // Setup initial password
    router.post('/setup', async (req, res) => {
        const { email, password } = req.body;
        
        if (email !== ADMIN_EMAIL) {
            return res.status(400).json({ error: 'Invalid email' });
        }
        
        if (!password || password.length < 6) {
            return res.status(400).json({ error: 'Password must be at least 6 characters' });
        }
        
        const connection = await pool.getConnection();
        try {
            // Create table if not exists
            await connection.execute(`
                CREATE TABLE IF NOT EXISTS admin_settings (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    setting_key VARCHAR(255) UNIQUE,
                    setting_value TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            `);
            
            // Check if password already exists
            const [existing] = await connection.execute(
                'SELECT * FROM admin_settings WHERE setting_key = ?',
                ['admin_password']
            );
            
            if (existing.length > 0) {
                return res.status(400).json({ error: 'Password already set' });
            }
            
            // Hash and save password
            const hashedPassword = encryption.hashPassword(password);
            await connection.execute(
                'INSERT INTO admin_settings (setting_key, setting_value) VALUES (?, ?)',
                ['admin_password', hashedPassword]
            );
            
            res.json({ success: true });
        } catch (error) {
            console.error('Setup error:', error);
            res.status(500).json({ error: 'Setup failed' });
        } finally {
            connection.release();
        }
    });

    // Login with password
    router.post('/login', async (req, res) => {
        const { email, password } = req.body;
        
        if (email !== ADMIN_EMAIL) {
            return res.status(401).json({ error: 'Invalid credentials' });
        }
        
        const connection = await pool.getConnection();
        try {
            const [rows] = await connection.execute(
                'SELECT setting_value FROM admin_settings WHERE setting_key = ?',
                ['admin_password']
            );
            
            if (rows.length === 0) {
                return res.status(400).json({ error: 'Setup required' });
            }
            
            const storedHash = rows[0].setting_value;
            if (!encryption.verifyPassword(password, storedHash)) {
                return res.status(401).json({ error: 'Invalid credentials' });
            }
            
            // Generate JWT
            const token = jwt.sign({ email: ADMIN_EMAIL }, JWT_SECRET, { expiresIn: '7d' });
            
            res.cookie('adminToken', token, {
                httpOnly: true,
                secure: true,
                sameSite: 'strict',
                maxAge: 7 * 24 * 60 * 60 * 1000 // 7 days
            });
            
            res.json({ success: true, token });
        } catch (error) {
            console.error('Login error:', error);
            res.status(500).json({ error: 'Login failed' });
        } finally {
            connection.release();
        }
    });

    // Google OAuth login for admin
    router.get('/google-auth', (req, res) => {
        const authUrl = new URL('https://accounts.google.com/o/oauth2/auth');
        authUrl.searchParams.set('client_id', config.google.clientId);
        authUrl.searchParams.set('redirect_uri', `${config.baseUrl}/admin/google-callback`);
        authUrl.searchParams.set('response_type', 'code');
        authUrl.searchParams.set('scope', 'email profile');
        authUrl.searchParams.set('access_type', 'offline');
        authUrl.searchParams.set('prompt', 'select_account');
        
        res.redirect(authUrl.toString());
    });

    // Google OAuth callback
    router.get('/google-callback', async (req, res) => {
        const { code, error } = req.query;
        
        if (error) {
            return res.redirect('/admin?error=' + encodeURIComponent(error));
        }
        
        try {
            // Exchange code for tokens
            const tokenResponse = await axios.post('https://oauth2.googleapis.com/token', {
                client_id: config.google.clientId,
                client_secret: config.google.clientSecret,
                code: code,
                grant_type: 'authorization_code',
                redirect_uri: `${config.baseUrl}/admin/google-callback`
            });
            
            const { access_token } = tokenResponse.data;
            
            // Get user info
            const userInfoResponse = await axios.get('https://www.googleapis.com/oauth2/v2/userinfo', {
                headers: { Authorization: `Bearer ${access_token}` }
            });
            
            const { email } = userInfoResponse.data;
            
            if (email !== ADMIN_EMAIL) {
                return res.redirect('/admin?error=' + encodeURIComponent('Unauthorized email'));
            }
            
            // Generate JWT
            const token = jwt.sign({ email }, JWT_SECRET, { expiresIn: '7d' });
            
            res.cookie('adminToken', token, {
                httpOnly: true,
                secure: true,
                sameSite: 'strict',
                maxAge: 7 * 24 * 60 * 60 * 1000
            });
            
            res.redirect('/admin');
        } catch (error) {
            console.error('Google auth error:', error.response?.data || error.message);
            res.redirect('/admin?error=' + encodeURIComponent('Authentication failed'));
        }
    });

    // Verify token endpoint
    router.get('/verify', verifyAdmin, (req, res) => {
        res.json({ valid: true, email: req.admin.email });
    });

    // Logout
    router.post('/logout', (req, res) => {
        res.clearCookie('adminToken');
        res.json({ success: true });
    });

    // Get dashboard summary
    router.get('/status', verifyAdmin, async (req, res) => {
        try {
            const status = await contactSaver.getStatus();
            res.json(status);
        } catch (error) {
            console.error('Status error:', error);
            res.status(500).json({ error: 'Failed to get status' });
        }
    });

    // Get all customers with stats
    router.get('/customers', verifyAdmin, async (req, res) => {
        try {
            const customers = await contactSaver.getCustomers();
            res.json(customers);
        } catch (error) {
            console.error('Customers error:', error);
            res.status(500).json({ error: 'Failed to get customers' });
        }
    });

    // Get specific customer details
    router.get('/customers/:phone', verifyAdmin, async (req, res) => {
        try {
            const { phone } = req.params;
            const details = await contactSaver.getCustomerDetails(phone);
            
            if (!details) {
                return res.status(404).json({ error: 'Customer not found' });
            }
            
            res.json(details);
        } catch (error) {
            console.error('Customer details error:', error);
            res.status(500).json({ error: 'Failed to get customer details' });
        }
    });

    // Get all campaigns
    router.get('/campaigns', verifyAdmin, async (req, res) => {
        try {
            const campaigns = await contactSaver.getCampaigns();
            res.json(campaigns);
        } catch (error) {
            console.error('Campaigns error:', error);
            res.status(500).json({ error: 'Failed to get campaigns' });
        }
    });

    // Get specific campaign details
    router.get('/campaigns/:name', verifyAdmin, async (req, res) => {
        try {
            const { name } = req.params;
            const details = await contactSaver.getCampaignDetails(decodeURIComponent(name));
            
            if (!details) {
                return res.status(404).json({ error: 'Campaign not found' });
            }
            
            res.json(details);
        } catch (error) {
            console.error('Campaign details error:', error);
            res.status(500).json({ error: 'Failed to get campaign details' });
        }
    });

    // Manually trigger contact saver
    router.post('/run-saver', verifyAdmin, async (req, res) => {
        try {
            // Run asynchronously
            contactSaver.run();
            res.json({ success: true, message: 'Contact saver started' });
        } catch (error) {
            console.error('Run saver error:', error);
            res.status(500).json({ error: 'Failed to start contact saver' });
        }
    });

    // Migrate existing tokens to encrypted format
    router.post('/migrate-tokens', verifyAdmin, async (req, res) => {
        const connection = await pool.getConnection();
        try {
            // Get all customers with tokens
            const [customers] = await connection.execute(`
                SELECT id, Phone, AccessToken, RefreshToken 
                FROM לקוחות 
                WHERE AccessToken IS NOT NULL OR RefreshToken IS NOT NULL
            `);
            
            let migrated = 0;
            let skipped = 0;
            
            for (const customer of customers) {
                try {
                    // Try to decrypt - if it works, it's already encrypted
                    const decryptedAccess = encryption.decrypt(customer.AccessToken);
                    if (decryptedAccess) {
                        skipped++;
                        continue;
                    }
                } catch (e) {
                    // Not encrypted, proceed with migration
                }
                
                // Encrypt tokens
                const encryptedAccess = customer.AccessToken ? encryption.encrypt(customer.AccessToken) : null;
                const encryptedRefresh = customer.RefreshToken ? encryption.encrypt(customer.RefreshToken) : null;
                
                await connection.execute(`
                    UPDATE לקוחות 
                    SET AccessToken = ?, RefreshToken = ?
                    WHERE id = ?
                `, [encryptedAccess, encryptedRefresh, customer.id]);
                
                migrated++;
            }
            
            res.json({ success: true, migrated, skipped, total: customers.length });
        } catch (error) {
            console.error('Migration error:', error);
            res.status(500).json({ error: 'Migration failed' });
        } finally {
            connection.release();
        }
    });

    // ==================== VCF DOWNLOAD & CONTACT MANAGEMENT ====================

    // Generate VCF content for contacts
    function generateVCF(contacts, labelName) {
        const formattedLabel = labelName.replace(/_/g, ' ');
        const now = new Date();
        const dateStr = `${String(now.getMonth() + 1).padStart(2, '0')}/${String(now.getFullYear()).slice(-2)}`;
        const fullLabel = `${formattedLabel} ${dateStr}`;
        
        // Characters to remove (same as googleContacts.js)
        const charsToRemove = ['=', '*', ',', '•', '°', '>', '<', '~', '@', '#', '$', '%', '^', '₪',
            '{', '}', '[', ']', '+', '&', '!', '|', '_', ';', '`', '\\', '\t', '?'];
        
        let vcf = '';
        for (const contact of contacts) {
            // Clean and format name (same rules as googleContacts.js)
            let name = contact.FullName || contact.name || 'צופה';
            
            // Remove special chars
            for (const char of charsToRemove) {
                name = name.split(char).join('');
            }
            name = name.replace(/[\r\n]+/g, ' '); // newlines to space
            name = name.replace(/^[0-9]+/, ''); // remove leading numbers
            name = name.replace(/^[\.\-_״"'\s]+/, ''); // remove leading special
            name = name.replace(/[\.\-_״"'\s\|!]+$/, ''); // remove trailing special
            name = name.replace(/[()]/g, ''); // remove parentheses
            name = name.replace(/\s{2,}/g, ' '); // double spaces
            name = name.trim();
            
            // Validate and set default if needed
            const hasValidChars = /[a-zA-Zא-ת]{2,}/.test(name);
            if (!hasValidChars || name.length < 2) {
                name = 'צופה';
            }
            if (name.length > 35) name = name.substring(0, 35).trim();
            
            // Add date suffix
            name = `${name} ${dateStr}`;
            
            // Format phone
            let phone = contact.Phone || contact.phone || '';
            phone = phone.replace(/[^0-9+]/g, '');
            if (!phone.startsWith('+')) {
                phone = '+' + phone;
            }
            
            vcf += 'BEGIN:VCARD\r\n';
            vcf += 'VERSION:3.0\r\n';
            vcf += `FN:${name}\r\n`;
            vcf += `N:;${name};;;\r\n`;
            vcf += `TEL;TYPE=CELL:${phone}\r\n`;
            vcf += `CATEGORIES:${fullLabel}\r\n`;
            vcf += `NOTE:${fullLabel}\r\n`;
            vcf += 'END:VCARD\r\n';
        }
        return vcf;
    }

    // Download VCF for a customer (all pending contacts)
    router.get('/customers/:phone/download-vcf', verifyAdmin, async (req, res) => {
        const connection = await pool.getConnection();
        try {
            const { phone } = req.params;
            const contacts = await contactSaver.getPendingContactsForCustomer(phone);
            
            if (!contacts || contacts.length === 0) {
                return res.status(404).json({ error: 'No pending contacts found' });
            }
            
            // Get customer info for filename
            const [custRows] = await connection.execute('SELECT FullName FROM לקוחות WHERE Phone = ?', [phone]);
            const customerName = custRows[0]?.FullName || phone;
            
            const vcf = generateVCF(contacts, 'אנשי_קשר');
            const filename = `contacts_${customerName.replace(/[^a-zA-Z0-9\u0590-\u05FF]/g, '_')}_${Date.now()}.vcf`;
            
            res.setHeader('Content-Type', 'text/vcard; charset=utf-8');
            res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(filename)}"`);
            res.send(vcf);
        } catch (error) {
            console.error('Download VCF error:', error);
            res.status(500).json({ error: 'Failed to generate VCF' });
        } finally {
            connection.release();
        }
    });

    // Download VCF for a specific campaign
    router.get('/campaigns/:name/customers/:phone/download-vcf', verifyAdmin, async (req, res) => {
        try {
            const { name, phone } = req.params;
            const campaignName = decodeURIComponent(name);
            const contacts = await contactSaver.getPendingContactsForCampaign(phone, campaignName);
            
            if (!contacts || contacts.length === 0) {
                return res.status(404).json({ error: 'No pending contacts found' });
            }
            
            const vcf = generateVCF(contacts, campaignName);
            const filename = `${campaignName.replace(/[^a-zA-Z0-9\u0590-\u05FF]/g, '_')}_${Date.now()}.vcf`;
            
            res.setHeader('Content-Type', 'text/vcard; charset=utf-8');
            res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(filename)}"`);
            res.send(vcf);
        } catch (error) {
            console.error('Download campaign VCF error:', error);
            res.status(500).json({ error: 'Failed to generate VCF' });
        }
    });

    // Mark contacts as not for saving (status 3) - for a customer
    router.post('/customers/:phone/mark-not-saving', verifyAdmin, async (req, res) => {
        try {
            const { phone } = req.params;
            const result = await contactSaver.markContactsAsNotSaving(phone);
            res.json({ success: true, ...result });
        } catch (error) {
            console.error('Mark not saving error:', error);
            res.status(500).json({ error: 'Failed to mark contacts' });
        }
    });

    // Mark contacts as not for saving (status 3) - for a specific campaign
    router.post('/campaigns/:name/customers/:phone/mark-not-saving', verifyAdmin, async (req, res) => {
        try {
            const { name, phone } = req.params;
            const campaignName = decodeURIComponent(name);
            const result = await contactSaver.markCampaignContactsAsNotSaving(phone, campaignName);
            res.json({ success: true, ...result });
        } catch (error) {
            console.error('Mark campaign not saving error:', error);
            res.status(500).json({ error: 'Failed to mark contacts' });
        }
    });

    // Mark contacts as saved (status 1) - for a customer (after VCF download)
    router.post('/customers/:phone/mark-saved', verifyAdmin, async (req, res) => {
        try {
            const { phone } = req.params;
            const result = await contactSaver.markContactsAsSaved(phone);
            res.json({ success: true, ...result });
        } catch (error) {
            console.error('Mark saved error:', error);
            res.status(500).json({ error: 'Failed to mark contacts' });
        }
    });

    // Mark contacts as saved (status 1) - for a specific campaign (after VCF download)
    router.post('/campaigns/:name/customers/:phone/mark-saved', verifyAdmin, async (req, res) => {
        try {
            const { name, phone } = req.params;
            const campaignName = decodeURIComponent(name);
            const result = await contactSaver.markCampaignContactsAsSaved(phone, campaignName);
            res.json({ success: true, ...result });
        } catch (error) {
            console.error('Mark campaign saved error:', error);
            res.status(500).json({ error: 'Failed to mark contacts' });
        }
    });

    // ==================== RECONNECTION LINKS ====================

    // Get reconnection link for a customer
    router.get('/customers/:phone/reconnect-link', verifyAdmin, async (req, res) => {
        const connection = await pool.getConnection();
        try {
            const { phone } = req.params;
            
            // Get customer info
            const [rows] = await connection.execute(
                'SELECT Phone, Email, FullName FROM לקוחות WHERE Phone = ?',
                [phone]
            );
            
            if (rows.length === 0) {
                return res.status(404).json({ error: 'Customer not found' });
            }
            
            const customer = rows[0];
            const baseUrl = config.baseUrl || 'https://registration.botomat.co.il';
            
            // Create pre-filled registration link
            const params = new URLSearchParams({
                phone: customer.Phone || '',
                email: customer.Email || '',
                name: customer.FullName || '',
                reconnect: '1'
            });
            
            const link = `${baseUrl}/register?${params.toString()}`;
            
            res.json({ 
                success: true, 
                link,
                customer: {
                    phone: customer.Phone,
                    email: customer.Email,
                    name: customer.FullName
                }
            });
        } catch (error) {
            console.error('Reconnect link error:', error);
            res.status(500).json({ error: 'Failed to generate link' });
        } finally {
            connection.release();
        }
    });

    // Send reconnection link to customer via WhatsApp
    router.post('/customers/:phone/send-reconnect', verifyAdmin, async (req, res) => {
        const connection = await pool.getConnection();
        try {
            const { phone } = req.params;
            
            // Get customer info
            const [rows] = await connection.execute(
                'SELECT Phone, Email, FullName FROM לקוחות WHERE Phone = ?',
                [phone]
            );
            
            if (rows.length === 0) {
                return res.status(404).json({ error: 'Customer not found' });
            }
            
            const customer = rows[0];
            const baseUrl = config.baseUrl || 'https://registration.botomat.co.il';
            
            // Create pre-filled registration link
            const params = new URLSearchParams({
                phone: customer.Phone || '',
                email: customer.Email || '',
                name: customer.FullName || '',
                reconnect: '1'
            });
            
            const link = `${baseUrl}/register?${params.toString()}`;
            
            // Format phone for WhatsApp
            let whatsappPhone = customer.Phone.replace(/[^0-9]/g, '');
            if (!whatsappPhone.startsWith('972')) {
                whatsappPhone = '972' + whatsappPhone.replace(/^0/, '');
            }
            
            // Send WhatsApp message
            const message = `שלום ${customer.FullName || ''},

נדרשת התחברות מחדש לחשבון הגוגל שלך לצורך שמירת אנשי קשר.

לחץ/י על הקישור להתחברות:
${link}

תודה!`;

            await axios.post(config.whatsapp.apiUrl, {
                chatId: `${whatsappPhone}@c.us`,
                text: message,
                session: config.whatsapp.session
            }, {
                headers: {
                    'accept': 'application/json',
                    'X-Api-Key': config.whatsapp.apiKey,
                    'Content-Type': 'application/json'
                }
            });
            
            res.json({ success: true, message: 'Link sent successfully' });
        } catch (error) {
            console.error('Send reconnect error:', error);
            res.status(500).json({ error: 'Failed to send link' });
        } finally {
            connection.release();
        }
    });

    // Get all customers with active errors (errors that weren't resolved by successful saves)
    router.get('/customers-with-errors', verifyAdmin, async (req, res) => {
        const connection = await pool.getConnection();
        try {
            // Get customers with errors - check if their last activity was an error (not a success)
            const [customers] = await connection.execute(`
                SELECT 
                    c.phone,
                    COALESCE(c.full_name, cust.FullName) as full_name,
                    COALESCE(c.email, cust.Email) as email,
                    c.last_error,
                    c.last_error_type,
                    c.google_contact_count,
                    c.updated_at
                FROM cs_customers c
                LEFT JOIN לקוחות cust ON cust.Phone = c.phone
                WHERE c.last_error IS NOT NULL
                ORDER BY c.updated_at DESC
            `);
            
            res.json(customers);
        } catch (error) {
            console.error('Get customers with errors:', error);
            res.status(500).json({ error: 'Failed to get customers' });
        } finally {
            connection.release();
        }
    });

    return router;
};
