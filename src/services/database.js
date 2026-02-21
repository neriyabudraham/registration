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
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_phone (phone),
                    INDEX idx_active (is_active)
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
    async logSaveAttempt(customerPhone, campaignName, contactPhone, contactName, status, errorMessage = null) {
        const connection = await this.pool.getConnection();
        try {
            await connection.execute(`
                INSERT INTO cs_save_log 
                (customer_phone, campaign_name, contact_phone, contact_name, status, error_message, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, NOW())
            `, [customerPhone, campaignName, contactPhone, contactName, status, errorMessage]);

            // Update hourly stats
            const hourTimestamp = new Date();
            hourTimestamp.setMinutes(0, 0, 0);

            if (status === 'saved') {
                await connection.execute(`
                    INSERT INTO cs_hourly_stats (customer_phone, hour_timestamp, saved_count)
                    VALUES (?, ?, 1)
                    ON DUPLICATE KEY UPDATE saved_count = saved_count + 1
                `, [customerPhone, hourTimestamp]);
            } else if (status === 'existed') {
                await connection.execute(`
                    INSERT INTO cs_hourly_stats (customer_phone, hour_timestamp, existed_count)
                    VALUES (?, ?, 1)
                    ON DUPLICATE KEY UPDATE existed_count = existed_count + 1
                `, [customerPhone, hourTimestamp]);
            } else if (status === 'error') {
                await connection.execute(`
                    INSERT INTO cs_hourly_stats (customer_phone, hour_timestamp, error_count)
                    VALUES (?, ?, 1)
                    ON DUPLICATE KEY UPDATE error_count = error_count + 1
                `, [customerPhone, hourTimestamp]);
            }
        } finally {
            connection.release();
        }
    }

    // Update customer error status
    async updateCustomerError(customerPhone, errorType, errorMessage, notified = false) {
        const connection = await this.pool.getConnection();
        try {
            await connection.execute(`
                UPDATE cs_customers 
                SET last_error = ?, last_error_type = ?, error_notified = ?, updated_at = NOW()
                WHERE phone = ?
            `, [errorMessage, errorType, notified, customerPhone]);
        } finally {
            connection.release();
        }
    }

    // Clear customer error
    async clearCustomerError(customerPhone) {
        const connection = await this.pool.getConnection();
        try {
            await connection.execute(`
                UPDATE cs_customers 
                SET last_error = NULL, last_error_type = NULL, error_notified = FALSE, updated_at = NOW()
                WHERE phone = ?
            `, [customerPhone]);
        } finally {
            connection.release();
        }
    }

    // Get all customers with summary stats
    async getCustomersWithStats() {
        const connection = await this.pool.getConnection();
        try {
            const [customers] = await connection.execute(`
                SELECT 
                    c.*,
                    COALESCE(SUM(s.total_contacts), 0) as total_contacts,
                    COALESCE(SUM(s.pending_count), 0) as total_pending,
                    COALESCE(SUM(s.saved_count), 0) as total_saved,
                    COALESCE(SUM(s.existed_count), 0) as total_existed,
                    COUNT(DISTINCT s.campaign_name) as campaign_count
                FROM cs_customers c
                LEFT JOIN cs_campaign_stats s ON c.phone = s.customer_phone
                WHERE c.is_active = TRUE
                GROUP BY c.id
                ORDER BY c.full_name, c.phone
            `);

            // Get last hour saves for each customer
            for (const customer of customers) {
                const [hourStats] = await connection.execute(`
                    SELECT COALESCE(SUM(saved_count), 0) as last_hour_saved
                    FROM cs_hourly_stats
                    WHERE customer_phone = ?
                    AND hour_timestamp >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
                `, [customer.phone]);
                
                customer.last_hour_saved = Number(hourStats[0]?.last_hour_saved) || 0;
            }

            return customers;
        } finally {
            connection.release();
        }
    }

    // Get customer details with all campaigns
    async getCustomerDetails(customerPhone) {
        const connection = await this.pool.getConnection();
        try {
            // Get customer info
            const [customers] = await connection.execute(`
                SELECT * FROM cs_customers WHERE phone = ?
            `, [customerPhone]);

            if (customers.length === 0) return null;

            const customer = customers[0];

            // Get campaign stats
            const [campaigns] = await connection.execute(`
                SELECT * FROM cs_campaign_stats 
                WHERE customer_phone = ?
                ORDER BY campaign_name
            `, [customerPhone]);

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
            const summary = {
                total_contacts: campaigns.reduce((sum, c) => sum + c.total_contacts, 0),
                total_pending: campaigns.reduce((sum, c) => sum + c.pending_count, 0),
                total_saved: campaigns.reduce((sum, c) => sum + c.saved_count, 0),
                total_existed: campaigns.reduce((sum, c) => sum + c.existed_count, 0),
                last_hour_saved: hourlyStats
                    .filter(h => new Date(h.hour_timestamp) >= new Date(Date.now() - 3600000))
                    .reduce((sum, h) => sum + h.saved_count, 0),
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

    // Get dashboard summary
    async getDashboardSummary() {
        const connection = await this.pool.getConnection();
        try {
            const [summary] = await connection.execute(`
                SELECT 
                    COUNT(DISTINCT c.phone) as total_customers,
                    SUM(CASE WHEN c.has_valid_tokens THEN 1 ELSE 0 END) as connected_customers,
                    SUM(CASE WHEN c.last_error IS NOT NULL THEN 1 ELSE 0 END) as customers_with_errors,
                    COALESCE(SUM(s.pending_count), 0) as total_pending,
                    COALESCE(SUM(s.saved_count), 0) as total_saved,
                    COALESCE(SUM(s.existed_count), 0) as total_existed
                FROM cs_customers c
                LEFT JOIN cs_campaign_stats s ON c.phone = s.customer_phone
                WHERE c.is_active = TRUE
            `);

            // Get last hour activity
            const [hourActivity] = await connection.execute(`
                SELECT 
                    COALESCE(SUM(saved_count), 0) as last_hour_saved,
                    COALESCE(SUM(error_count), 0) as last_hour_errors
                FROM cs_hourly_stats
                WHERE hour_timestamp >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
            `);

            return {
                ...summary[0],
                last_hour_saved: Number(hourActivity[0]?.last_hour_saved) || 0,
                last_hour_errors: Number(hourActivity[0]?.last_hour_errors) || 0
            };
        } finally {
            connection.release();
        }
    }
}

module.exports = DatabaseService;
