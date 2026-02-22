const GoogleContactsService = require('./googleContacts');
const EncryptionService = require('./encryption');
const DatabaseService = require('./database');
const axios = require('axios');

class ContactSaverService {
    constructor(pool, config) {
        this.pool = pool;
        this.config = config;
        this.encryption = new EncryptionService(config.encryptionKey);
        this.db = new DatabaseService(pool);
        this.isRunning = false;
        this.rateLimitedCustomers = new Map(); // customerId -> resumeTime
        // Note: error notifications are now tracked in database (cs_customers.error_notified)
        // so they persist across restarts
        this.initialized = false;
    }

    // Initialize database tables
    async initialize() {
        if (this.initialized) return;
        try {
            await this.db.initializeTables();
            await this.db.syncCustomers();
            this.initialized = true;
            console.log('ContactSaverService initialized');
        } catch (error) {
            console.error('Failed to initialize ContactSaverService:', error);
        }
    }

    // Get all tables that need processing
    async getContactTables() {
        const connection = await this.pool.getConnection();
        try {
            const [tables] = await connection.execute(`
                SELECT TABLE_NAME 
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = ? 
                AND (TABLE_NAME LIKE 'הגרלה%' OR TABLE_NAME LIKE 'הגרלת%' OR TABLE_NAME LIKE 'שמירת\\_אנשי\\_קשר%')
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

    // Get customer by phone from לקוחות table
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
            return [];
        } finally {
            connection.release();
        }
    }

    // Get all contacts from a table for stats
    async getTableStats(tableName, customerPhone) {
        const connection = await this.pool.getConnection();
        try {
            // Check if column exists
            const [columns] = await connection.execute(`
                SELECT COLUMN_NAME 
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
            `, [this.config.database, tableName, customerPhone]);
            
            if (columns.length === 0) return null;

            const [stats] = await connection.execute(`
                SELECT 
                    COUNT(*) as total,
                    CAST(SUM(CASE WHEN \`${customerPhone}\` = 0 THEN 1 ELSE 0 END) AS SIGNED) as pending,
                    CAST(SUM(CASE WHEN \`${customerPhone}\` = 1 THEN 1 ELSE 0 END) AS SIGNED) as saved,
                    CAST(SUM(CASE WHEN \`${customerPhone}\` = 2 THEN 1 ELSE 0 END) AS SIGNED) as existed,
                    CAST(SUM(CASE WHEN \`${customerPhone}\` IN (3, 4) THEN 1 ELSE 0 END) AS SIGNED) as ignored
                FROM \`${tableName}\`
            `);
            
            return {
                total: Number(stats[0].total) || 0,
                pending: Number(stats[0].pending) || 0,
                saved: Number(stats[0].saved) || 0,
                existed: Number(stats[0].existed) || 0,
                ignored: Number(stats[0].ignored) || 0
            };
        } catch (error) {
            return null;
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

    // Send WhatsApp notification for errors (persisted in database)
    async sendErrorNotification(customerPhone, errorType, errorMessage, customerName = null) {
        // Only send WhatsApp for critical errors (not temporary rate limits)
        const criticalErrors = ['TOKEN_INVALID', 'PERMISSION_DENIED', 'CONTACT_LIMIT_EXCEEDED', 'CONTACT_LIMIT_MAX'];
        
        if (!criticalErrors.includes(errorType)) {
            // Just update database with error, don't send WhatsApp
            await this.db.updateCustomerError(customerPhone, errorType, errorMessage, false);
            return;
        }
        
        // Check if we already sent notification for this exact error type (stored in DB)
        const existingError = await this.db.getCustomerErrorState(customerPhone);
        
        const alreadyNotified = existingError && 
            existingError.last_error_type === errorType && 
            (existingError.error_notified === 1 || existingError.error_notified === true || Number(existingError.error_notified) === 1);
        
        console.log(`Notification check for ${customerPhone}: type=${errorType}, existing=${existingError?.last_error_type}, notified=${existingError?.error_notified}, skip=${alreadyNotified}`);
        
        if (alreadyNotified) {
            // Already notified for this error type - don't send again, don't update
            console.log(`Skipping duplicate WhatsApp for ${customerPhone} - already notified for ${errorType}`);
            return;
        }
        
        // Update database with error and mark as notified BEFORE sending
        await this.db.updateCustomerError(customerPhone, errorType, errorMessage, true);
        
        // Get customer name if not provided
        if (!customerName) {
            const customer = await this.getCustomerByPhone(customerPhone);
            customerName = customer?.FullName || 'לא ידוע';
        }
        
        const errorMessages = {
            'TOKEN_INVALID': 'טוקן לא תקין - נדרשת התחברות מחדש',
            'PERMISSION_DENIED': 'חסרות הרשאות לשמירת אנשי קשר',
            'CONTACT_LIMIT_EXCEEDED': 'החשבון הגיע למגבלת אנשי קשר - יש למחוק אנשי קשר',
            'CONTACT_LIMIT_MAX': 'חריגה ממגבלת 25,000 אנשי קשר - יש לחבר חשבון חדש או למחוק אנשי קשר'
        };

        const message = `⚠️ *שגיאה בשמירת אנשי קשר*

*לקוח:* ${customerName}
*טלפון:* ${customerPhone}
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
            console.log(`WhatsApp notification sent for ${customerPhone}: ${errorType}`);
        } catch (error) {
            console.error('Failed to send WhatsApp notification:', error.message);
        }
    }

    // Clear error notification (called when save succeeds)
    clearErrorNotification(customerPhone) {
        this.db.clearCustomerError(customerPhone);
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
        
        // Get contact count in the account
        const contactCount = await googleService.getContactCount();
        if (contactCount !== null) {
            await this.db.updateCustomerContactCount(customerPhone, contactCount);
            
            // Check if account has too many contacts (>25000)
            if (contactCount >= 25000) {
                await this.sendErrorNotification(
                    customerPhone, 
                    'CONTACT_LIMIT_MAX', 
                    `החשבון מכיל ${contactCount} אנשי קשר - חריגה ממגבלת 25,000. יש לחבר חשבון חדש או למחוק אנשי קשר`,
                    customer.FullName
                );
                return { status: 'contact_limit_max', contactCount };
            }
        }
        
        let savedCount = 0;
        let existedCount = 0;
        let errorCount = 0;
        
        for (const tableName of tables) {
            // Update stats for this campaign
            const stats = await this.getTableStats(tableName, customerPhone);
            if (stats) {
                await this.db.updateCampaignStats(customerPhone, tableName, stats);
            }
            
            const contacts = await this.getContactsToSave(tableName, customerPhone);
            
            for (const contact of contacts) {
                try {
                    const originalName = contact.FullName || '';
                    const result = await googleService.saveContactWithLabel(
                        originalName,
                        contact.Phone,
                        tableName
                    );
                    
                    const newStatus = result.status === 'created' ? 1 : 2;
                    await this.updateContactStatus(tableName, contact.Phone, customerPhone, newStatus);
                    
                    // Log to our database with both original and saved name
                    const logStatus = result.status === 'created' ? 'saved' : 'existed';
                    const savedName = result.savedName || originalName;
                    await this.db.logSaveAttempt(customerPhone, tableName, contact.Phone, originalName, logStatus, null, savedName);
                    
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
                    
                    const errorCode = error.code || 'UNKNOWN_ERROR';
                    const errorMsg = error.message || 'שגיאה לא ידועה';
                    
                    // Log error
                    await this.db.logSaveAttempt(
                        customerPhone, tableName, contact.Phone, contact.FullName, 
                        'error', `${errorCode}: ${errorMsg}`
                    );
                    
                    // Handle rate limit (temporary)
                    if (errorCode === 'RATE_LIMIT_TEMPORARY') {
                        const waitMs = (error.retryAfter || 60) * 1000;
                        this.rateLimitedCustomers.set(customerPhone, Date.now() + waitMs);
                        await this.sendErrorNotification(customerPhone, errorCode, errorMsg, customer.FullName);
                        break;
                    }
                    
                    // Handle contact limit exceeded (permanent until contacts deleted)
                    if (errorCode === 'CONTACT_LIMIT_EXCEEDED') {
                        await this.sendErrorNotification(customerPhone, errorCode, errorMsg, customer.FullName);
                        break; // Stop processing this customer
                    }
                    
                    // Handle token/permission errors
                    if (errorCode === 'TOKEN_INVALID' || errorCode === 'PERMISSION_DENIED') {
                        await this.sendErrorNotification(customerPhone, errorCode, errorMsg, customer.FullName);
                        break;
                    }
                    
                    console.error(`Error saving contact for ${customerPhone}:`, errorCode, errorMsg);
                }
            }
            
            // Update final stats after processing
            const finalStats = await this.getTableStats(tableName, customerPhone);
            if (finalStats) {
                await this.db.updateCampaignStats(customerPhone, tableName, finalStats);
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
        
        // Initialize if needed
        await this.initialize();
        
        this.isRunning = true;
        console.log('Starting contact saver run...');
        
        try {
            // Sync customers first
            await this.db.syncCustomers();
            
            // Get all relevant tables
            const tables = await this.getContactTables();
            console.log(`Found ${tables.length} tables to process`);
            
            // Get all phone columns (customers)
            const allCustomerPhones = new Set();
            for (const table of tables) {
                const phoneColumns = await this.getPhoneColumns(table);
                phoneColumns.forEach(p => allCustomerPhones.add(p));
            }
            
            const customerPhoneArray = Array.from(allCustomerPhones);
            console.log(`Found ${customerPhoneArray.length} customers to process`);
            
            // Process customers in parallel batches of 5
            const BATCH_SIZE = 5;
            for (let i = 0; i < customerPhoneArray.length; i += BATCH_SIZE) {
                const batch = customerPhoneArray.slice(i, i + BATCH_SIZE);
                const results = await Promise.allSettled(
                    batch.map(phone => this.processCustomer(phone, tables))
                );
                
                results.forEach((result, idx) => {
                    const phone = batch[idx];
                    if (result.status === 'fulfilled') {
                        console.log(`Customer ${phone}: ${JSON.stringify(result.value)}`);
                    } else {
                        console.error(`Customer ${phone} failed:`, result.reason);
                    }
                });
            }
            
        } catch (error) {
            console.error('Contact saver error:', error);
        } finally {
            this.isRunning = false;
        }
    }

    // Get status using our database
    async getStatus() {
        await this.initialize();
        return await this.db.getDashboardSummary();
    }

    // Get all customers with stats
    async getCustomers() {
        await this.initialize();
        return await this.db.getCustomersWithStats();
    }

    // Get customer details
    async getCustomerDetails(customerPhone) {
        await this.initialize();
        return await this.db.getCustomerDetails(customerPhone);
    }

    // Get all campaigns
    async getCampaigns() {
        await this.initialize();
        return await this.db.getCampaignsWithStats();
    }

    // Get campaign details
    async getCampaignDetails(campaignName) {
        await this.initialize();
        return await this.db.getCampaignDetails(campaignName);
    }

    // ==================== VCF & CONTACT MANAGEMENT ====================

    // Get all pending contacts for a customer across all campaigns
    async getPendingContactsForCustomer(customerPhone) {
        const connection = await this.pool.getConnection();
        try {
            const tables = await this.getContactTables();
            const allContacts = [];

            for (const tableName of tables) {
                const phoneColumns = await this.getPhoneColumns(tableName);
                if (!phoneColumns.includes(customerPhone)) continue;

                const [rows] = await connection.execute(`
                    SELECT Phone, FullName FROM \`${tableName}\` WHERE \`${customerPhone}\` = 0
                `);
                
                rows.forEach(row => {
                    allContacts.push({
                        ...row,
                        campaign: tableName
                    });
                });
            }

            return allContacts;
        } finally {
            connection.release();
        }
    }

    // Get pending contacts for a specific campaign
    async getPendingContactsForCampaign(customerPhone, campaignName) {
        const connection = await this.pool.getConnection();
        try {
            const phoneColumns = await this.getPhoneColumns(campaignName);
            if (!phoneColumns.includes(customerPhone)) return [];

            const [rows] = await connection.execute(`
                SELECT Phone, FullName FROM \`${campaignName}\` WHERE \`${customerPhone}\` = 0
            `);

            return rows;
        } finally {
            connection.release();
        }
    }

    // Mark all pending contacts as "not for saving" (status 3) for a customer
    async markContactsAsNotSaving(customerPhone) {
        const connection = await this.pool.getConnection();
        try {
            const tables = await this.getContactTables();
            let totalMarked = 0;

            for (const tableName of tables) {
                const phoneColumns = await this.getPhoneColumns(tableName);
                if (!phoneColumns.includes(customerPhone)) continue;

                const [result] = await connection.execute(`
                    UPDATE \`${tableName}\` SET \`${customerPhone}\` = 3 WHERE \`${customerPhone}\` = 0
                `);
                totalMarked += result.affectedRows;
            }

            return { marked: totalMarked };
        } finally {
            connection.release();
        }
    }

    // Mark contacts as "not for saving" for a specific campaign
    async markCampaignContactsAsNotSaving(customerPhone, campaignName) {
        const connection = await this.pool.getConnection();
        try {
            const phoneColumns = await this.getPhoneColumns(campaignName);
            if (!phoneColumns.includes(customerPhone)) return { marked: 0 };

            const [result] = await connection.execute(`
                UPDATE \`${campaignName}\` SET \`${customerPhone}\` = 3 WHERE \`${customerPhone}\` = 0
            `);

            return { marked: result.affectedRows };
        } finally {
            connection.release();
        }
    }

    // Mark all pending contacts as "saved" (status 1) for a customer
    async markContactsAsSaved(customerPhone) {
        const connection = await this.pool.getConnection();
        try {
            const tables = await this.getContactTables();
            let totalMarked = 0;

            for (const tableName of tables) {
                const phoneColumns = await this.getPhoneColumns(tableName);
                if (!phoneColumns.includes(customerPhone)) continue;

                const [result] = await connection.execute(`
                    UPDATE \`${tableName}\` SET \`${customerPhone}\` = 1 WHERE \`${customerPhone}\` = 0
                `);
                totalMarked += result.affectedRows;
            }

            return { marked: totalMarked };
        } finally {
            connection.release();
        }
    }

    // Mark contacts as "saved" for a specific campaign
    async markCampaignContactsAsSaved(customerPhone, campaignName) {
        const connection = await this.pool.getConnection();
        try {
            const phoneColumns = await this.getPhoneColumns(campaignName);
            if (!phoneColumns.includes(customerPhone)) return { marked: 0 };

            const [result] = await connection.execute(`
                UPDATE \`${campaignName}\` SET \`${customerPhone}\` = 1 WHERE \`${customerPhone}\` = 0
            `);

            return { marked: result.affectedRows };
        } finally {
            connection.release();
        }
    }
}

module.exports = ContactSaverService;
