// Database setup and management for the contact saver system

class DatabaseService {
    constructor(pool) {
        this.pool = pool;
    }

    // Initialize our own tables
    async initializeTables() {
        const connection = await this.pool.getConnection();
        try {
            // Customer tracking table
            await connection.execute(`
                CREATE TABLE IF NOT EXISTS cs_customers (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    phone VARCHAR(20) UNIQUE NOT NULL,
                    email VARCHAR(255),
                    full_name VARCHAR(255),
                    group_id VARCHAR(255),
                    has_valid_tokens BOOLEAN DEFAULT FALSE,
                    last_token_refresh DATETIME,
                    last_error VARCHAR(500),
                    last_error_type VARCHAR(50),
                    error_notified BOOLEAN DEFAULT FALSE,
                    google_contact_count INT DEFAULT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_phone (phone),
                    INDEX idx_active (is_active)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            `);
            
            // Add google_contact_count column if it doesn't exist (for existing tables)
            try {
                const [cols] = await connection.execute(`
                    SELECT COLUMN_NAME FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'cs_customers' AND COLUMN_NAME = 'google_contact_count'
                `);
                if (cols.length === 0) {
                    await connection.execute(`ALTER TABLE cs_customers ADD COLUMN google_contact_count INT DEFAULT NULL`);
                }
            } catch (e) { console.log('Column google_contact_count may already exist'); }
            
            // Add error_notified column if it doesn't exist (for existing tables)
            try {
                const [cols] = await connection.execute(`
                    SELECT COLUMN_NAME FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'cs_customers' AND COLUMN_NAME = 'error_notified'
                `);
                if (cols.length === 0) {
                    await connection.execute(`ALTER TABLE cs_customers ADD COLUMN error_notified BOOLEAN DEFAULT FALSE`);
                    console.log('Added error_notified column to cs_customers');
                }
            } catch (e) { console.log('Column error_notified may already exist'); }

            // Account tracking table (per email, for multi-account support)
            await connection.execute(`
                CREATE TABLE IF NOT EXISTS cs_accounts (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    phone VARCHAR(20) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    google_contact_count INT DEFAULT NULL,
                    last_error VARCHAR(500),
                    last_error_type VARCHAR(50),
                    error_notified BOOLEAN DEFAULT FALSE,
                    has_valid_tokens BOOLEAN DEFAULT TRUE,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_phone_email (phone, email),
                    INDEX idx_phone (phone),
                    INDEX idx_email (email)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            `);

            // Campaign stats per customer
            await connection.execute(`
                CREATE TABLE IF NOT EXISTS cs_campaign_stats (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_phone VARCHAR(20) NOT NULL,
                    campaign_name VARCHAR(255) NOT NULL,
                    total_contacts INT DEFAULT 0,
                    pending_count INT DEFAULT 0,
                    saved_count INT DEFAULT 0,
                    existed_count INT DEFAULT 0,
                    ignored_count INT DEFAULT 0,
                    last_processed DATETIME,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_customer_campaign (customer_phone, campaign_name),
                    INDEX idx_customer (customer_phone),
                    INDEX idx_campaign (campaign_name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            `);

            // Save activity log
            await connection.execute(`
                CREATE TABLE IF NOT EXISTS cs_save_log (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_phone VARCHAR(20) NOT NULL,
                    campaign_name VARCHAR(255) NOT NULL,
                    contact_phone VARCHAR(20) NOT NULL,
                    contact_name VARCHAR(255),
                    saved_name VARCHAR(255),
                    status ENUM('pending', 'saved', 'existed', 'error', 'skipped') DEFAULT 'pending',
                    error_message VARCHAR(500),
                    processed_at DATETIME,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_customer (customer_phone),
                    INDEX idx_campaign (campaign_name),
                    INDEX idx_status (status),
                    INDEX idx_processed (processed_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            `);
            
            // Add saved_name column if it doesn't exist (for existing tables)
            try {
                const [cols] = await connection.execute(`
                    SELECT COLUMN_NAME FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'cs_save_log' AND COLUMN_NAME = 'saved_name'
                `);
                if (cols.length === 0) {
                    await connection.execute(`ALTER TABLE cs_save_log ADD COLUMN saved_name VARCHAR(255) AFTER contact_name`);
                }
            } catch (e) { console.log('Column saved_name may already exist'); }

            // Hourly stats for graphs
            await connection.execute(`
                CREATE TABLE IF NOT EXISTS cs_hourly_stats (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_phone VARCHAR(20) NOT NULL,
                    hour_timestamp DATETIME NOT NULL,
                    saved_count INT DEFAULT 0,
                    existed_count INT DEFAULT 0,
                    error_count INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_customer_hour (customer_phone, hour_timestamp),
                    INDEX idx_customer (customer_phone),
                    INDEX idx_hour (hour_timestamp)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            `);

            console.log('Contact saver tables initialized');
        } finally {
            connection.release();
        }
    }

    // Sync customers from לקוחות table
    async syncCustomers() {
        const connection = await this.pool.getConnection();
        try {
            const [customers] = await connection.execute(`
                SELECT Phone, Email, FullName, GroupID, AccessToken, RefreshToken
                FROM לקוחות
                WHERE Phone IS NOT NULL AND Phone != ''
            `);

            for (const customer of customers) {
                await connection.execute(`
                    INSERT INTO cs_customers (phone, email, full_name, group_id, has_valid_tokens)
                    VALUES (?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                        email = VALUES(email),
                        full_name = VALUES(full_name),
                        group_id = VALUES(group_id),
                        has_valid_tokens = VALUES(has_valid_tokens),
                        updated_at = NOW()
                `, [
                    customer.Phone,
                    customer.Email,
                    customer.FullName,
                    customer.GroupID,
                    !!(customer.AccessToken && customer.RefreshToken)
                ]);
            }

            return customers.length;
        } finally {
            connection.release();
        }
    }

    // Update campaign stats for a customer
    async updateCampaignStats(customerPhone, campaignName, stats) {
        const connection = await this.pool.getConnection();
        try {
            await connection.execute(`
                INSERT INTO cs_campaign_stats 
                (customer_phone, campaign_name, total_contacts, pending_count, saved_count, existed_count, ignored_count, last_processed)
                VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE
                    total_contacts = VALUES(total_contacts),
                    pending_count = VALUES(pending_count),
                    saved_count = VALUES(saved_count),
                    existed_count = VALUES(existed_count),
                    ignored_count = VALUES(ignored_count),
                    last_processed = NOW(),
                    updated_at = NOW()
            `, [
                customerPhone,
                campaignName,
                stats.total || 0,
                stats.pending || 0,
                stats.saved || 0,
                stats.existed || 0,
                stats.ignored || 0
            ]);
        } finally {
            connection.release();
        }
    }

    // Log a save attempt
    async logSaveAttempt(customerPhone, campaignName, contactPhone, contactName, status, errorMessage = null, savedName = null) {
        const connection = await this.pool.getConnection();
        try {
            // Ensure saved_name column exists
            const [cols] = await connection.execute(`
                SELECT COLUMN_NAME FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'cs_save_log' AND COLUMN_NAME = 'saved_name'
            `);
            if (cols.length === 0) {
                await connection.execute(`ALTER TABLE cs_save_log ADD COLUMN saved_name VARCHAR(255) AFTER contact_name`);
            }
            
            await connection.execute(`
                INSERT INTO cs_save_log 
                (customer_phone, campaign_name, contact_phone, contact_name, saved_name, status, error_message, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
            `, [customerPhone, campaignName, contactPhone, contactName, savedName, status, errorMessage]);

            // Update hourly stats
            const hourTimestamp = new Date();
            hourTimestamp.setMinutes(0, 0, 0);

            if (status === 'saved') {
                await connection.execute(`
                    INSERT INTO cs_hourly_stats (customer_phone, hour_timestamp, saved_count)
                    VALUES (?, ?, 1)
                    ON DUPLICATE KEY UPDATE saved_count = saved_count + 1
                `, [customerPhone, hourTimestamp]);
                
                // Clear any previous error since save succeeded
                await connection.execute(`
                    UPDATE cs_customers 
                    SET last_error = NULL, last_error_type = NULL, has_valid_tokens = 1, updated_at = NOW()
                    WHERE phone = ?
                `, [customerPhone]);
            } else if (status === 'existed') {
                await connection.execute(`
                    INSERT INTO cs_hourly_stats (customer_phone, hour_timestamp, existed_count)
                    VALUES (?, ?, 1)
                    ON DUPLICATE KEY UPDATE existed_count = existed_count + 1
                `, [customerPhone, hourTimestamp]);
                
                // Clear any previous error since connection works
                await connection.execute(`
                    UPDATE cs_customers 
                    SET last_error = NULL, last_error_type = NULL, has_valid_tokens = 1, updated_at = NOW()
                    WHERE phone = ?
                `, [customerPhone]);
            } else if (status === 'error') {
                await connection.execute(`
                    INSERT INTO cs_hourly_stats (customer_phone, hour_timestamp, error_count)
                    VALUES (?, ?, 1)
                    ON DUPLICATE KEY UPDATE error_count = error_count + 1
                `, [customerPhone, hourTimestamp]);
                
                // NOTE: We do NOT update cs_customers here anymore
                // Error handling is done by sendErrorNotification which properly manages error_notified flag
            }
        } finally {
            connection.release();
        }
    }

    // Update customer error status (INSERT if not exists)
    async updateCustomerError(customerPhone, errorType, errorMessage, notified = false) {
        const connection = await this.pool.getConnection();
        try {
            // First try to get customer info from לקוחות table
            const [custRows] = await connection.execute(`
                SELECT FullName, Email FROM לקוחות WHERE Phone = ?
            `, [customerPhone]);
            
            const custInfo = custRows[0] || {};
            const isTokenError = errorType === 'TOKEN_INVALID' || errorType === 'PERMISSION_DENIED';
            const notifiedInt = notified ? 1 : 0; // Convert boolean to int for MySQL
            
            console.log(`Updating error for ${customerPhone}: ${errorType}, notified=${notifiedInt}`);
            
            await connection.execute(`
                INSERT INTO cs_customers (phone, full_name, email, last_error, last_error_type, error_notified, has_valid_tokens)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    last_error = VALUES(last_error),
                    last_error_type = VALUES(last_error_type),
                    error_notified = VALUES(error_notified),
                    has_valid_tokens = VALUES(has_valid_tokens),
                    updated_at = NOW()
            `, [customerPhone, custInfo.FullName || '', custInfo.Email || '', errorMessage, errorType, notifiedInt, isTokenError ? 0 : 1]);
            
            console.log(`Error updated successfully for ${customerPhone}, error_notified=${notifiedInt}`);
        } catch (err) {
            console.error(`Failed to update error for ${customerPhone}:`, err.message);
        } finally {
            connection.release();
        }
    }

    // Clear customer error (called when save succeeds)
    async clearCustomerError(customerPhone) {
        const connection = await this.pool.getConnection();
        try {
            await connection.execute(`
                UPDATE cs_customers 
                SET last_error = NULL, last_error_type = NULL, error_notified = 0, has_valid_tokens = 1, updated_at = NOW()
                WHERE phone = ?
            `, [customerPhone]);
        } finally {
            connection.release();
        }
    }

    // Get customer error state (for checking if notification was already sent)
    async getCustomerErrorState(customerPhone) {
        const connection = await this.pool.getConnection();
        try {
            const [rows] = await connection.execute(`
                SELECT last_error, last_error_type, error_notified 
                FROM cs_customers 
                WHERE phone = ?
            `, [customerPhone]);
            return rows[0] || null;
        } finally {
            connection.release();
        }
    }

    // Update customer contact count
    async updateCustomerContactCount(customerPhone, contactCount) {
        const connection = await this.pool.getConnection();
        try {
            // First ensure the column exists
            const [cols] = await connection.execute(`
                SELECT COLUMN_NAME FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'cs_customers' AND COLUMN_NAME = 'google_contact_count'
            `);
            if (cols.length === 0) {
                await connection.execute(`ALTER TABLE cs_customers ADD COLUMN google_contact_count INT DEFAULT NULL`);
            }
            
            await connection.execute(`
                UPDATE cs_customers 
                SET google_contact_count = ?, updated_at = NOW()
                WHERE phone = ?
            `, [contactCount, customerPhone]);
        } finally {
            connection.release();
        }
    }

    // Update contact count for specific account (phone + email)
    async updateAccountContactCount(phone, email, contactCount) {
        const connection = await this.pool.getConnection();
        try {
            await connection.execute(`
                INSERT INTO cs_accounts (phone, email, google_contact_count, last_updated)
                VALUES (?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE
                    google_contact_count = VALUES(google_contact_count),
                    last_updated = NOW()
            `, [phone, email, contactCount]);
        } finally {
            connection.release();
        }
    }

    // Get all accounts for a customer with their contact counts
    async getCustomerAccounts(phone) {
        const connection = await this.pool.getConnection();
        try {
            const [rows] = await connection.execute(`
                SELECT 
                    l.Email as email,
                    CASE WHEN l.AccessToken IS NOT NULL AND l.RefreshToken IS NOT NULL THEN 1 ELSE 0 END as has_tokens,
                    COALESCE(a.google_contact_count, 0) as google_contact_count,
                    a.last_error,
                    a.last_error_type,
                    a.error_notified
                FROM לקוחות l
                LEFT JOIN cs_accounts a ON a.phone = l.Phone AND a.email = l.Email
                WHERE l.Phone = ?
            `, [phone]);
            return rows;
        } finally {
            connection.release();
        }
    }

    // Update account error status
    async updateAccountError(phone, email, errorType, errorMessage, notified = false) {
        const connection = await this.pool.getConnection();
        try {
            const notifiedInt = notified ? 1 : 0;
            const hasValidTokens = ['TOKEN_INVALID', 'PERMISSION_DENIED'].includes(errorType) ? 0 : 1;
            
            await connection.execute(`
                INSERT INTO cs_accounts (phone, email, last_error, last_error_type, error_notified, has_valid_tokens, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE
                    last_error = VALUES(last_error),
                    last_error_type = VALUES(last_error_type),
                    error_notified = VALUES(error_notified),
                    has_valid_tokens = VALUES(has_valid_tokens),
                    last_updated = NOW()
            `, [phone, email, errorMessage, errorType, notifiedInt, hasValidTokens]);
        } finally {
            connection.release();
        }
    }

    // Clear account error
    async clearAccountError(phone, email) {
        const connection = await this.pool.getConnection();
        try {
            await connection.execute(`
                UPDATE cs_accounts 
                SET last_error = NULL, last_error_type = NULL, error_notified = 0, has_valid_tokens = 1, last_updated = NOW()
                WHERE phone = ? AND email = ?
            `, [phone, email]);
        } finally {
            connection.release();
        }
    }

    // Get all campaign tables (with fresh schema)
    async getCampaignTables() {
        const connection = await this.pool.getConnection();
        try {
            // Force refresh schema cache
            await connection.execute('SET SESSION information_schema_stats_expiry = 0');
            
            const [tables] = await connection.execute(`
                SELECT TABLE_NAME 
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE()
                AND (TABLE_NAME LIKE 'הגרלה%' OR TABLE_NAME LIKE 'הגרלת%' OR TABLE_NAME LIKE 'שמירת\\_אנשי\\_קשר%')
            `);
            console.log(`Found ${tables.length} campaign tables`);
            return tables.map(t => t.TABLE_NAME);
        } finally {
            connection.release();
        }
    }

    // Get phone columns from a table (customer phone numbers) - fresh query
    async getPhoneColumns(tableName) {
        const connection = await this.pool.getConnection();
        try {
            // Force refresh schema cache
            await connection.execute('SET SESSION information_schema_stats_expiry = 0');
            
            const [columns] = await connection.execute(`
                SELECT COLUMN_NAME 
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = ? 
                AND COLUMN_NAME REGEXP '^972[0-9]{9}$'
            `, [tableName]);
            return columns.map(c => c.COLUMN_NAME);
        } finally {
            connection.release();
        }
    }

    // Get REAL-TIME stats for a customer from source tables
    async getRealTimeCustomerStats(customerPhone) {
        const connection = await this.pool.getConnection();
        try {
            // Force refresh schema cache
            await connection.execute('SET SESSION information_schema_stats_expiry = 0');
            
            const tables = await this.getCampaignTables();
            const campaigns = [];
            let totalPending = 0, totalSaved = 0, totalExisted = 0, totalContacts = 0, totalSkipped = 0;

            for (const table of tables) {
                // Check if this customer has a column in this table (fresh query each time)
                const [columns] = await connection.execute(`
                    SELECT COLUMN_NAME 
                    FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = ? 
                    AND COLUMN_NAME = ?
                `, [table, customerPhone]);
                
                if (columns.length === 0) continue;

                try {
                    const [stats] = await connection.execute(`
                        SELECT 
                            COUNT(*) as total,
                            CAST(SUM(CASE WHEN \`${customerPhone}\` = 0 THEN 1 ELSE 0 END) AS SIGNED) as pending,
                            CAST(SUM(CASE WHEN \`${customerPhone}\` = 1 THEN 1 ELSE 0 END) AS SIGNED) as saved,
                            CAST(SUM(CASE WHEN \`${customerPhone}\` = 2 THEN 1 ELSE 0 END) AS SIGNED) as existed,
                            CAST(SUM(CASE WHEN \`${customerPhone}\` IN (3, 4) THEN 1 ELSE 0 END) AS SIGNED) as skipped
                        FROM \`${table}\`
                    `);

                    const s = stats[0];
                    const pending = Number(s.pending) || 0;
                    const saved = Number(s.saved) || 0;
                    const existed = Number(s.existed) || 0;
                    const skipped = Number(s.skipped) || 0;
                    const total = Number(s.total) || 0;

                    campaigns.push({
                        campaign_name: table,
                        total_contacts: total,
                        pending_count: pending,
                        saved_count: saved,
                        existed_count: existed,
                        skipped_count: skipped
                    });

                    totalPending += pending;
                    totalSaved += saved;
                    totalExisted += existed;
                    totalSkipped += skipped;
                    totalContacts += total;
                } catch (e) {
                    console.error(`Error getting stats for ${table}/${customerPhone}:`, e.message);
                }
            }

            return {
                campaigns,
                total_contacts: totalContacts,
                total_pending: totalPending,
                total_saved: totalSaved,
                total_existed: totalExisted,
                total_skipped: totalSkipped,
                campaign_count: campaigns.length
            };
        } finally {
            connection.release();
        }
    }

    // Get all customers with REAL-TIME stats from source tables
    async getCustomersWithStats() {
        const connection = await this.pool.getConnection();
        try {
            // First, get all unique customer phones from campaign tables
            const tables = await this.getCampaignTables();
            const allCustomerPhones = new Set();

            for (const table of tables) {
                const phoneColumns = await this.getPhoneColumns(table);
                phoneColumns.forEach(p => allCustomerPhones.add(p));
            }

            if (allCustomerPhones.size === 0) {
                return [];
            }

            const customers = [];

            for (const phone of allCustomerPhones) {
                // Get ALL customer accounts from לקוחות table with per-account stats
                const [custRows] = await connection.execute(`
                    SELECT l.Phone, l.Email, l.FullName, l.GroupID, 
                           CASE WHEN l.AccessToken IS NOT NULL AND l.RefreshToken IS NOT NULL THEN 1 ELSE 0 END as has_valid_tokens,
                           a.google_contact_count as account_contact_count,
                           a.last_error as account_error,
                           a.last_error_type as account_error_type
                    FROM לקוחות l
                    LEFT JOIN cs_accounts a ON a.phone = l.Phone AND a.email = l.Email
                    WHERE l.Phone = ?
                `, [phone]);

                // Get real-time stats
                const stats = await this.getRealTimeCustomerStats(phone);

                // Get error info from our tracking table (legacy, for backward compat)
                const [errorRows] = await connection.execute(`
                    SELECT last_error, last_error_type, google_contact_count, has_valid_tokens as cs_valid_tokens FROM cs_customers WHERE phone = ?
                `, [phone]);

                // Also check for the most recent activity in save_log
                const [recentActivity] = await connection.execute(`
                    SELECT status, error_message, processed_at FROM cs_save_log 
                    WHERE customer_phone = ? 
                    ORDER BY processed_at DESC LIMIT 1
                `, [phone]);

                // Get last hour saved from our log
                const [hourStats] = await connection.execute(`
                    SELECT COALESCE(SUM(saved_count), 0) as last_hour_saved
                    FROM cs_hourly_stats
                    WHERE customer_phone = ?
                    AND hour_timestamp >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
                `, [phone]);

                const custInfo = custRows[0] || {};
                const errorInfo = errorRows[0] || {};
                const lastActivity = recentActivity[0];
                
                // Collect all accounts (emails) for this phone with per-account data
                const accounts = custRows.map(row => ({
                    email: row.Email,
                    has_tokens: row.has_valid_tokens === 1,
                    google_contact_count: row.account_contact_count || 0,
                    last_error: row.account_error,
                    last_error_type: row.account_error_type
                }));
                
                // Calculate total contacts across all accounts
                const totalContactCount = accounts.reduce((sum, acc) => sum + (acc.google_contact_count || 0), 0);
                
                // Determine error state:
                // 1. If cs_customers has error, use it
                // 2. If not but last activity was an error, use that
                let lastError = errorInfo.last_error;
                let lastErrorType = errorInfo.last_error_type;
                
                if (!lastError && lastActivity?.status === 'error' && lastActivity.error_message) {
                    lastError = lastActivity.error_message;
                    lastErrorType = lastActivity.error_message?.split(':')[0] || 'UNKNOWN_ERROR';
                }
                
                // Check if it's a token error
                const isTokenError = lastErrorType === 'TOKEN_INVALID' || lastErrorType === 'PERMISSION_DENIED';
                
                // has_valid_tokens: true if ANY account has valid tokens and no critical error
                const anyAccountHasTokens = accounts.some(a => a.has_tokens);
                const hasValidTokens = errorInfo.cs_valid_tokens === 0 ? false : 
                                       (isTokenError ? false : anyAccountHasTokens);

                // Check for any account-level errors
                const accountWithError = accounts.find(a => a.last_error);
                if (accountWithError && !lastError) {
                    lastError = accountWithError.last_error;
                    lastErrorType = accountWithError.last_error_type;
                }

                customers.push({
                    phone: phone,
                    email: custInfo.Email,
                    full_name: custInfo.FullName,
                    group_id: custInfo.GroupID,
                    accounts: accounts, // Array of all linked accounts with per-account contact counts
                    accounts_count: accounts.length,
                    has_valid_tokens: hasValidTokens,
                    last_error: lastError,
                    last_error_type: lastErrorType,
                    google_contact_count: totalContactCount, // Sum of all accounts
                    total_contacts: stats.total_contacts,
                    total_pending: stats.total_pending,
                    total_saved: stats.total_saved,
                    total_existed: stats.total_existed,
                    total_skipped: stats.total_skipped || 0,
                    campaign_count: stats.campaign_count,
                    last_hour_saved: Number(hourStats[0]?.last_hour_saved) || 0
                });
            }

            // Sort: prioritize customers with pending contacts
            customers.sort((a, b) => {
                const aScore = (a.total_pending > 0 ? 10000 : 0) + a.total_saved + (a.has_valid_tokens ? 100 : 0);
                const bScore = (b.total_pending > 0 ? 10000 : 0) + b.total_saved + (b.has_valid_tokens ? 100 : 0);
                return bScore - aScore;
            });

            return customers;
        } finally {
            connection.release();
        }
    }

    // Get all campaigns with REAL-TIME stats from source tables
    async getCampaignsWithStats() {
        const connection = await this.pool.getConnection();
        try {
            // Force refresh schema cache
            await connection.execute('SET SESSION information_schema_stats_expiry = 0');
            
            const tables = await this.getCampaignTables();
            const campaigns = [];

            for (const table of tables) {
                const phoneColumns = await this.getPhoneColumns(table);
                if (phoneColumns.length === 0) continue;

                let totalPending = 0, totalSaved = 0, totalExisted = 0, totalSkipped = 0, totalContacts = 0;

                // Get stats for all customers in this campaign
                for (const phone of phoneColumns) {
                    try {
                        const [stats] = await connection.execute(`
                            SELECT 
                                COUNT(*) as total,
                                CAST(SUM(CASE WHEN \`${phone}\` = 0 THEN 1 ELSE 0 END) AS SIGNED) as pending,
                                CAST(SUM(CASE WHEN \`${phone}\` = 1 THEN 1 ELSE 0 END) AS SIGNED) as saved,
                                CAST(SUM(CASE WHEN \`${phone}\` = 2 THEN 1 ELSE 0 END) AS SIGNED) as existed,
                                CAST(SUM(CASE WHEN \`${phone}\` IN (3, 4) THEN 1 ELSE 0 END) AS SIGNED) as skipped
                            FROM \`${table}\`
                        `);

                        totalPending += Number(stats[0].pending) || 0;
                        totalSaved += Number(stats[0].saved) || 0;
                        totalExisted += Number(stats[0].existed) || 0;
                        totalSkipped += Number(stats[0].skipped) || 0;
                        totalContacts = Number(stats[0].total) || 0; // Same for all columns
                    } catch (e) {}
                }

                campaigns.push({
                    campaign_name: table,
                    customer_count: phoneColumns.length,
                    total_contacts: totalContacts,
                    total_pending: totalPending,
                    total_saved: totalSaved,
                    total_existed: totalExisted,
                    total_skipped: totalSkipped
                });
            }

            // Sort by pending (most urgent first)
            campaigns.sort((a, b) => b.total_pending - a.total_pending);
            
            return campaigns;
        } finally {
            connection.release();
        }
    }

    // Get campaign details with all customers (REAL-TIME)
    async getCampaignDetails(campaignName) {
        const connection = await this.pool.getConnection();
        try {
            // Force refresh schema cache
            await connection.execute('SET SESSION information_schema_stats_expiry = 0');
            
            const phoneColumns = await this.getPhoneColumns(campaignName);
            if (phoneColumns.length === 0) return null;

            const customers = [];
            let summaryPending = 0, summarySaved = 0, summaryExisted = 0, summarySkipped = 0, summaryTotal = 0;

            for (const phone of phoneColumns) {
                // Get customer info
                const [custRows] = await connection.execute(`
                    SELECT Phone, Email, FullName,
                           CASE WHEN AccessToken IS NOT NULL AND RefreshToken IS NOT NULL THEN 1 ELSE 0 END as has_valid_tokens
                    FROM לקוחות WHERE Phone = ?
                `, [phone]);

                // Get error info
                const [errorRows] = await connection.execute(`
                    SELECT last_error, last_error_type FROM cs_customers WHERE phone = ?
                `, [phone]);

                // Get stats for this customer in this campaign
                try {
                    const [stats] = await connection.execute(`
                        SELECT 
                            COUNT(*) as total,
                            CAST(SUM(CASE WHEN \`${phone}\` = 0 THEN 1 ELSE 0 END) AS SIGNED) as pending,
                            CAST(SUM(CASE WHEN \`${phone}\` = 1 THEN 1 ELSE 0 END) AS SIGNED) as saved,
                            CAST(SUM(CASE WHEN \`${phone}\` = 2 THEN 1 ELSE 0 END) AS SIGNED) as existed,
                            CAST(SUM(CASE WHEN \`${phone}\` IN (3, 4) THEN 1 ELSE 0 END) AS SIGNED) as skipped
                        FROM \`${campaignName}\`
                    `);

                    const s = stats[0];
                    const pending = Number(s.pending) || 0;
                    const saved = Number(s.saved) || 0;
                    const existed = Number(s.existed) || 0;
                    const skipped = Number(s.skipped) || 0;
                    const total = Number(s.total) || 0;

                    const custInfo = custRows[0] || {};
                    const errorInfo = errorRows[0] || {};

                    customers.push({
                        customer_phone: phone,
                        full_name: custInfo.FullName,
                        email: custInfo.Email,
                        has_valid_tokens: custInfo.has_valid_tokens === 1,
                        last_error: errorInfo.last_error,
                        last_error_type: errorInfo.last_error_type,
                        total_contacts: total,
                        pending_count: pending,
                        saved_count: saved,
                        existed_count: existed,
                        skipped_count: skipped
                    });

                    summaryPending += pending;
                    summarySaved += saved;
                    summaryExisted += existed;
                    summarySkipped += skipped;
                    summaryTotal = total; // Same for all
                } catch (e) {}
            }

            // Sort by pending
            customers.sort((a, b) => b.pending_count - a.pending_count);
            
            const summary = {
                total_customers: customers.length,
                total_contacts: summaryTotal,
                total_pending: summaryPending,
                total_saved: summarySaved,
                total_existed: summaryExisted,
                total_skipped: summarySkipped
            };
            
            return { campaign_name: campaignName, customers, summary };
        } finally {
            connection.release();
        }
    }

    // Get customer details with all campaigns (REAL-TIME from source tables)
    async getCustomerDetails(customerPhone) {
        const connection = await this.pool.getConnection();
        try {
            // Get ALL customer accounts from לקוחות table (multiple emails per phone)
            const [custRows] = await connection.execute(`
                SELECT Phone, Email, FullName, GroupID, 
                       CASE WHEN AccessToken IS NOT NULL AND RefreshToken IS NOT NULL THEN 1 ELSE 0 END as has_valid_tokens
                FROM לקוחות WHERE Phone = ?
            `, [customerPhone]);

            // Get per-account stats from cs_accounts
            const [accountStats] = await connection.execute(`
                SELECT email, google_contact_count, last_error, last_error_type, has_valid_tokens as account_valid
                FROM cs_accounts WHERE phone = ?
            `, [customerPhone]);
            
            // Create a map of account stats by email
            const accountStatsMap = {};
            for (const as of accountStats) {
                accountStatsMap[as.email] = as;
            }

            const custInfo = custRows[0] || {};
            
            // Collect all accounts with per-account data
            const accounts = custRows.map(row => {
                const stats = accountStatsMap[row.Email] || {};
                return {
                    email: row.Email,
                    has_tokens: row.has_valid_tokens === 1,
                    google_contact_count: stats.google_contact_count || 0,
                    last_error: stats.last_error,
                    last_error_type: stats.last_error_type
                };
            });
            
            // Calculate total contact count across all accounts
            const totalContactCount = accounts.reduce((sum, acc) => sum + (acc.google_contact_count || 0), 0);
            
            // Any account has valid tokens
            const anyAccountHasTokens = accounts.some(a => a.has_tokens);
            
            // Find first error from any account
            const accountWithError = accounts.find(a => a.last_error);

            const customer = {
                phone: customerPhone,
                email: custInfo.Email,
                full_name: custInfo.FullName,
                group_id: custInfo.GroupID,
                accounts: accounts,
                accounts_count: accounts.length,
                has_valid_tokens: anyAccountHasTokens,
                last_error: accountWithError?.last_error || null,
                last_error_type: accountWithError?.last_error_type || null,
                google_contact_count: totalContactCount
            };

            // Get REAL-TIME campaign stats
            const stats = await this.getRealTimeCustomerStats(customerPhone);
            const campaigns = stats.campaigns;

            // Get recent activity (last 50)
            const [recentActivity] = await connection.execute(`
                SELECT * FROM cs_save_log
                WHERE customer_phone = ?
                ORDER BY created_at DESC
                LIMIT 50
            `, [customerPhone]);

            // Get hourly stats for last 24 hours
            const [hourlyStats] = await connection.execute(`
                SELECT 
                    hour_timestamp,
                    saved_count,
                    existed_count,
                    error_count
                FROM cs_hourly_stats
                WHERE customer_phone = ?
                AND hour_timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                ORDER BY hour_timestamp
            `, [customerPhone]);

            // Calculate summary
            const lastHourSaved = hourlyStats
                .filter(h => new Date(h.hour_timestamp) >= new Date(Date.now() - 3600000))
                .reduce((sum, h) => sum + (h.saved_count || 0), 0);

            const summary = {
                total_contacts: stats.total_contacts,
                total_pending: stats.total_pending,
                total_saved: stats.total_saved,
                total_existed: stats.total_existed,
                total_skipped: stats.total_skipped || 0,
                last_hour_saved: lastHourSaved,
                has_error: !!customer.last_error
            };

            return {
                customer,
                campaigns,
                recentActivity,
                hourlyStats,
                summary
            };
        } finally {
            connection.release();
        }
    }

    // Get dashboard summary (REAL-TIME from source tables)
    async getDashboardSummary() {
        const connection = await this.pool.getConnection();
        try {
            // Get all customers with real-time stats
            const customers = await this.getCustomersWithStats();

            let totalPending = 0, totalSaved = 0, totalExisted = 0, totalSkipped = 0;
            let connectedCustomers = 0, customersWithErrors = 0;

            for (const c of customers) {
                totalPending += c.total_pending || 0;
                totalSaved += c.total_saved || 0;
                totalExisted += c.total_existed || 0;
                totalSkipped += c.total_skipped || 0;
                if (c.has_valid_tokens) connectedCustomers++;
                if (c.last_error) customersWithErrors++;
            }

            // Get last hour activity from log
            const [hourActivity] = await connection.execute(`
                SELECT 
                    COALESCE(SUM(saved_count), 0) as last_hour_saved,
                    COALESCE(SUM(error_count), 0) as last_hour_errors
                FROM cs_hourly_stats
                WHERE hour_timestamp >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
            `);

            return {
                total_customers: customers.length,
                connected_customers: connectedCustomers,
                customers_with_errors: customersWithErrors,
                total_pending: totalPending,
                total_saved: totalSaved,
                total_existed: totalExisted,
                total_skipped: totalSkipped,
                last_hour_saved: Number(hourActivity[0]?.last_hour_saved) || 0,
                last_hour_errors: Number(hourActivity[0]?.last_hour_errors) || 0
            };
        } finally {
            connection.release();
        }
    }
}

module.exports = DatabaseService;
