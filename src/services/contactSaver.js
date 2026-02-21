const GoogleContactsService = require('./googleContacts');
const EncryptionService = require('./encryption');
const axios = require('axios');

class ContactSaverService {
    constructor(pool, config) {
        this.pool = pool;
        this.config = config;
        this.encryption = new EncryptionService(config.encryptionKey);
        this.isRunning = false;
        this.rateLimitedCustomers = new Map(); // customerId -> resumeTime
        this.errorNotifications = new Set(); // Track sent notifications to avoid duplicates
    }

    // Get all tables that need processing
    async getContactTables() {
        const connection = await this.pool.getConnection();
        try {
            const [tables] = await connection.execute(`
                SELECT TABLE_NAME 
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = ? 
                AND (TABLE_NAME LIKE 'הגרלה%' OR TABLE_NAME LIKE 'הגרלת%' OR TABLE_NAME LIKE 'שמירת_אנשי_קשר%')
            `, [this.config.database]);
            
            return tables.map(t => t.TABLE_NAME);
        } finally {
            connection.release();
        }
    }

    // Get phone number columns from a table
    async getPhoneColumns(tableName) {
        const connection = await this.pool.getConnection();
        try {
            const [columns] = await connection.execute(`
                SELECT COLUMN_NAME 
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = ? 
                AND TABLE_NAME = ? 
                AND COLUMN_NAME REGEXP '^972[0-9]{9}$'
            `, [this.config.database, tableName]);
            
            return columns.map(c => c.COLUMN_NAME);
        } finally {
            connection.release();
        }
    }

    // Get customer by phone
    async getCustomerByPhone(phone) {
        const connection = await this.pool.getConnection();
        try {
            const [rows] = await connection.execute(`
                SELECT * FROM לקוחות WHERE Phone = ? LIMIT 1
            `, [phone]);
            
            if (rows.length === 0) return null;
            
            const customer = rows[0];
            
            // Decrypt tokens if encrypted
            if (customer.AccessToken) {
                const decrypted = this.encryption.decrypt(customer.AccessToken);
                customer.AccessToken = decrypted || customer.AccessToken;
            }
            if (customer.RefreshToken) {
                const decrypted = this.encryption.decrypt(customer.RefreshToken);
                customer.RefreshToken = decrypted || customer.RefreshToken;
            }
            
            return customer;
        } finally {
            connection.release();
        }
    }

    // Update contact status in table
    async updateContactStatus(tableName, primaryPhone, customerPhone, status) {
        const connection = await this.pool.getConnection();
        try {
            // Use backticks for column name since it's a phone number
            await connection.execute(
                `UPDATE \`${tableName}\` SET \`${customerPhone}\` = ?, UpdateTime = NOW() WHERE \`Phone\` = ?`,
                [status, primaryPhone]
            );
        } finally {
            connection.release();
        }
    }

    // Get contacts to save from a table for a specific customer
    async getContactsToSave(tableName, customerPhone) {
        const connection = await this.pool.getConnection();
        try {
            const [rows] = await connection.execute(
                `SELECT \`Phone\`, FullName, \`${customerPhone}\` as status FROM \`${tableName}\` WHERE \`${customerPhone}\` = 0`
            );
            return rows;
        } catch (error) {
            // Column might not exist
            return [];
        } finally {
            connection.release();
        }
    }

    // Update customer tokens in database
    async updateCustomerTokens(phone, accessToken, refreshToken, expiresIn) {
        const connection = await this.pool.getConnection();
        try {
            const encryptedAccess = this.encryption.encrypt(accessToken);
            const encryptedRefresh = this.encryption.encrypt(refreshToken);
            
            await connection.execute(`
                UPDATE לקוחות 
                SET AccessToken = ?, RefreshToken = ?, ExpirationTime = ?
                WHERE Phone = ?
            `, [encryptedAccess, encryptedRefresh, expiresIn, phone]);
        } finally {
            connection.release();
        }
    }

    // Send WhatsApp notification for errors
    async sendErrorNotification(customerPhone, errorType, errorMessage) {
        const notificationKey = `${customerPhone}:${errorType}`;
        
        // Don't send duplicate notifications
        if (this.errorNotifications.has(notificationKey)) {
            return;
        }
        
        this.errorNotifications.add(notificationKey);
        
        const errorMessages = {
            'TOKEN_INVALID': 'טוקן לא תקין - נדרשת התחברות מחדש',
            'PERMISSION_DENIED': 'חסרות הרשאות לשמירת אנשי קשר',
            'RATE_LIMIT': 'הגעת למגבלת שמירה - ממתין',
            'UNKNOWN_ERROR': 'שגיאה לא ידועה'
        };

        const message = `⚠️ *שגיאה בשמירת אנשי קשר*

*לקוח:* ${customerPhone}
*סוג שגיאה:* ${errorMessages[errorType] || errorType}
*פרטים:* ${errorMessage || 'אין פרטים נוספים'}

נדרש טיפול!`;

        try {
            await axios.post(this.config.whatsapp.apiUrl, {
                chatId: this.config.whatsapp.chatId,
                text: message,
                session: this.config.whatsapp.session
            }, {
                headers: {
                    'accept': 'application/json',
                    'X-Api-Key': this.config.whatsapp.apiKey,
                    'Content-Type': 'application/json'
                }
            });
        } catch (error) {
            console.error('Failed to send WhatsApp notification:', error.message);
        }
    }

    // Clear error notification (when issue is resolved)
    clearErrorNotification(customerPhone, errorType) {
        this.errorNotifications.delete(`${customerPhone}:${errorType}`);
    }

    // Process a single customer
    async processCustomer(customerPhone, tables) {
        // Check if rate limited
        const resumeTime = this.rateLimitedCustomers.get(customerPhone);
        if (resumeTime && Date.now() < resumeTime) {
            return { status: 'rate_limited', resumeAt: resumeTime };
        }
        
        // Get customer data
        const customer = await this.getCustomerByPhone(customerPhone);
        if (!customer) {
            return { status: 'customer_not_found' };
        }
        
        if (!customer.AccessToken || !customer.RefreshToken) {
            return { status: 'no_tokens' };
        }
        
        // Create Google Contacts service
        const googleService = new GoogleContactsService(
            customer.AccessToken,
            customer.RefreshToken,
            this.config.google.clientId,
            this.config.google.clientSecret
        );
        
        let savedCount = 0;
        let existedCount = 0;
        let errorCount = 0;
        
        for (const tableName of tables) {
            const contacts = await this.getContactsToSave(tableName, customerPhone);
            
            for (const contact of contacts) {
                try {
                    const result = await googleService.saveContactWithLabel(
                        contact.FullName || 'ללא שם',
                        contact.Phone,
                        tableName // Use table name as label
                    );
                    
                    // Update status based on result
                    const newStatus = result.status === 'created' ? 1 : 2;
                    await this.updateContactStatus(tableName, contact.Phone, customerPhone, newStatus);
                    
                    if (result.status === 'created') {
                        savedCount++;
                    } else {
                        existedCount++;
                    }
                    
                    // Update tokens if refreshed
                    if (googleService.accessToken !== customer.AccessToken) {
                        await this.updateCustomerTokens(
                            customerPhone,
                            googleService.accessToken,
                            googleService.refreshToken,
                            3600
                        );
                    }
                    
                    // Small delay to avoid rate limits
                    await new Promise(r => setTimeout(r, 200));
                    
                } catch (error) {
                    errorCount++;
                    
                    if (error.code === 'RATE_LIMIT') {
                        // Set rate limit wait time
                        const waitMs = (error.retryAfter || 60) * 1000;
                        this.rateLimitedCustomers.set(customerPhone, Date.now() + waitMs);
                        await this.sendErrorNotification(customerPhone, 'RATE_LIMIT', `ממתין ${error.retryAfter} שניות`);
                        break;
                    }
                    
                    if (error.code === 'TOKEN_INVALID' || error.code === 'PERMISSION_DENIED') {
                        await this.sendErrorNotification(customerPhone, error.code, error.message);
                        break;
                    }
                    
                    console.error(`Error saving contact for ${customerPhone}:`, error);
                }
            }
        }
        
        // Clear rate limit notification if we're past it
        if (!this.rateLimitedCustomers.has(customerPhone)) {
            this.clearErrorNotification(customerPhone, 'RATE_LIMIT');
        }
        
        return { status: 'processed', saved: savedCount, existed: existedCount, errors: errorCount };
    }

    // Main processing loop
    async run() {
        if (this.isRunning) {
            console.log('Contact saver already running, skipping');
            return;
        }
        
        this.isRunning = true;
        console.log('Starting contact saver run...');
        
        try {
            // Get all relevant tables
            const tables = await this.getContactTables();
            console.log(`Found ${tables.length} tables to process`);
            
            // Get all phone columns (customers)
            const allCustomerPhones = new Set();
            for (const table of tables) {
                const phoneColumns = await this.getPhoneColumns(table);
                phoneColumns.forEach(p => allCustomerPhones.add(p));
            }
            
            console.log(`Found ${allCustomerPhones.size} customers to process`);
            
            // Process each customer
            for (const customerPhone of allCustomerPhones) {
                const result = await this.processCustomer(customerPhone, tables);
                console.log(`Customer ${customerPhone}: ${JSON.stringify(result)}`);
            }
            
        } catch (error) {
            console.error('Contact saver error:', error);
        } finally {
            this.isRunning = false;
        }
    }

    // Get status for admin panel
    async getStatus() {
        const tables = await this.getContactTables();
        const stats = {
            tables: tables.length,
            customers: [],
            rateLimited: []
        };
        
        // Get customer stats
        const connection = await this.pool.getConnection();
        try {
            const [customers] = await connection.execute(`
                SELECT Phone, Email, FullName, 
                       CASE WHEN AccessToken IS NOT NULL AND RefreshToken IS NOT NULL THEN 1 ELSE 0 END as hasTokens
                FROM לקוחות
            `);
            
            for (const customer of customers) {
                const customerStats = {
                    phone: customer.Phone,
                    email: customer.Email,
                    name: customer.FullName,
                    hasTokens: customer.hasTokens === 1,
                    pending: 0,
                    saved: 0,
                    existed: 0
                };
                
                // Count contacts per status for this customer
                for (const table of tables) {
                    try {
                        const [counts] = await connection.execute(`
                            SELECT 
                                SUM(CASE WHEN \`${customer.Phone}\` = 0 THEN 1 ELSE 0 END) as pending,
                                SUM(CASE WHEN \`${customer.Phone}\` = 1 THEN 1 ELSE 0 END) as saved,
                                SUM(CASE WHEN \`${customer.Phone}\` = 2 THEN 1 ELSE 0 END) as existed
                            FROM \`${table}\`
                        `);
                        
                        if (counts[0]) {
                            customerStats.pending += counts[0].pending || 0;
                            customerStats.saved += counts[0].saved || 0;
                            customerStats.existed += counts[0].existed || 0;
                        }
                    } catch (e) {
                        // Column might not exist
                    }
                }
                
                stats.customers.push(customerStats);
            }
            
            // Get rate limited customers
            for (const [phone, resumeTime] of this.rateLimitedCustomers) {
                if (Date.now() < resumeTime) {
                    stats.rateLimited.push({
                        phone,
                        resumeAt: new Date(resumeTime).toISOString()
                    });
                }
            }
            
        } finally {
            connection.release();
        }
        
        return stats;
    }
}

module.exports = ContactSaverService;
