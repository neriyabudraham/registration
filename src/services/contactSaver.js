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

    // Get customer by phone from לקוחות table (first account)
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

    // Get ALL accounts for a customer (multiple Google accounts)
    async getAllCustomerAccounts(phone) {
        const connection = await this.pool.getConnection();
        try {
            const [rows] = await connection.execute(`
                SELECT * FROM לקוחות WHERE Phone = ? AND AccessToken IS NOT NULL AND RefreshToken IS NOT NULL
            `, [phone]);
            
            // Decrypt tokens for all accounts
            return rows.map(customer => {
                if (customer.AccessToken) {
                    const decrypted = this.encryption.decrypt(customer.AccessToken);
                    customer.AccessToken = decrypted || customer.AccessToken;
                }
                if (customer.RefreshToken) {
                    const decrypted = this.encryption.decrypt(customer.RefreshToken);
                    customer.RefreshToken = decrypted || customer.RefreshToken;
                }
                return customer;
            });
        } finally {
            connection.release();
        }
    }

    // Create Google services for all accounts
    createGoogleServices(accounts) {
        return accounts.map(account => ({
            email: account.Email,
            fullName: account.FullName,
            service: new GoogleContactsService(
                account.AccessToken,
                account.RefreshToken,
                this.config.google.clientId,
                this.config.google.clientSecret
            )
        }));
    }

    // Check if contact exists in ANY of the accounts
    async contactExistsInAnyAccount(googleServices, phone) {
        for (const { service, email } of googleServices) {
            try {
                const exists = await service.searchContactByPhone(phone);
                if (exists) {
                    return { exists: true, inAccount: email };
                }
            } catch (err) {
                // Skip failed accounts
            }
        }
        return { exists: false };
    }

    // Get account with least contacts (for saving)
    async getBestAccountForSaving(googleServices) {
        let bestAccount = null;
        let minContacts = Infinity;
        
        for (const gs of googleServices) {
            try {
                const count = await gs.service.getContactCount();
                if (count !== null && count < minContacts && count < 25000) {
                    minContacts = count;
                    bestAccount = gs;
                }
            } catch (err) {
                // Skip failed accounts
            }
        }
        
        return bestAccount;
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

    // Update customer tokens in database (optionally by email for multi-account)
    async updateCustomerTokens(phone, accessToken, refreshToken, expiresIn, email = null) {
        const connection = await this.pool.getConnection();
        try {
            const encryptedAccess = this.encryption.encrypt(accessToken);
            const encryptedRefresh = this.encryption.encrypt(refreshToken);
            
            if (email) {
                // Update specific account by email
                await connection.execute(`
                    UPDATE לקוחות 
                    SET AccessToken = ?, RefreshToken = ?, ExpirationTime = ?
                    WHERE Phone = ? AND Email = ?
                `, [encryptedAccess, encryptedRefresh, expiresIn, phone, email]);
            } else {
                // Update first account for phone
                await connection.execute(`
                    UPDATE לקוחות 
                    SET AccessToken = ?, RefreshToken = ?, ExpirationTime = ?
                    WHERE Phone = ? LIMIT 1
                `, [encryptedAccess, encryptedRefresh, expiresIn, phone]);
            }
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
        
        // Get ALL accounts for this customer
        const accounts = await this.getAllCustomerAccounts(customerPhone);
        if (accounts.length === 0) {
            return { status: 'no_tokens' };
        }
        
        const customer = accounts[0]; // Primary account for name/info
        
        // Create Google services for all accounts
        const googleServices = this.createGoogleServices(accounts);
        console.log(`Customer ${customerPhone} has ${accounts.length} account(s): ${accounts.map(a => a.Email).join(', ')}`);
        
        // Get total contact count across all accounts
        let totalContactCount = 0;
        let allAccountsFull = true;
        
        for (const gs of googleServices) {
            try {
                const count = await gs.service.getContactCount();
                if (count !== null) {
                    totalContactCount += count;
                    if (count < 25000) {
                        allAccountsFull = false;
                    }
                }
            } catch (err) {
                // Account might have invalid token
            }
        }
        
        await this.db.updateCustomerContactCount(customerPhone, totalContactCount);
        
        // Check if ALL accounts are full
        if (allAccountsFull && accounts.length > 0) {
            await this.sendErrorNotification(
                customerPhone, 
                'CONTACT_LIMIT_MAX', 
                `כל החשבונות מלאים (סה"כ ${totalContactCount} אנשי קשר). יש לחבר חשבון חדש או למחוק אנשי קשר`,
                customer.FullName
            );
            return { status: 'contact_limit_max', contactCount: totalContactCount };
        }
        
        // Get best account for saving (least contacts, under 25k)
        const bestAccount = await this.getBestAccountForSaving(googleServices);
        if (!bestAccount) {
            return { status: 'no_valid_account' };
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
                    
                    // First check if contact exists in ANY of the customer's accounts
                    const existsCheck = await this.contactExistsInAnyAccount(googleServices, contact.Phone);
                    
                    let result;
                    if (existsCheck.exists) {
                        // Contact already exists in one of the accounts
                        result = { status: 'existed', inAccount: existsCheck.inAccount };
                    } else {
                        // Save to the best account (least contacts)
                        result = await bestAccount.service.saveContactWithLabel(
                            originalName,
                            contact.Phone,
                            tableName
                        );
                        if (result.status === 'created') {
                            result.savedToAccount = bestAccount.email;
                        }
                    }
                    
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
                    
                    // Update tokens if refreshed (for the account we used)
                    if (result.status === 'created' && bestAccount.service.accessToken) {
                        // Find the original account to update
                        const accountToUpdate = accounts.find(a => a.Email === bestAccount.email);
                        if (accountToUpdate && bestAccount.service.accessToken !== accountToUpdate.AccessToken) {
                            await this.updateCustomerTokens(
                                customerPhone,
                                bestAccount.service.accessToken,
                                bestAccount.service.refreshToken,
                                3600,
                                bestAccount.email
                            );
                        }
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
        
        // Note: We no longer clear errors here - errors are cleared only when save SUCCEEDS (in logSaveAttempt)
        // This ensures error_notified stays true until the customer reconnects and saves successfully
        
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

    // Periodic job to update contact counts for all customers with valid tokens
    async updateAllContactCounts() {
        console.log('Starting contact count update...');
        
        const connection = await this.pool.getConnection();
        try {
            // Get customers with valid tokens, excluding those with known token errors
            const [customers] = await connection.execute(`
                SELECT l.Phone, l.FullName, l.AccessToken, l.RefreshToken 
                FROM לקוחות l
                LEFT JOIN cs_customers c ON c.phone = l.Phone
                WHERE l.AccessToken IS NOT NULL AND l.RefreshToken IS NOT NULL
                AND (c.last_error_type IS NULL OR c.last_error_type NOT IN ('TOKEN_INVALID', 'PERMISSION_DENIED'))
            `);
            
            if (customers.length === 0) {
                console.log('No customers with valid tokens to update');
                return;
            }
            
            console.log(`Updating contact counts for ${customers.length} customers`);
            let updated = 0;
            let errors = 0;
            
            for (const customer of customers) {
                try {
                    // Decrypt tokens
                    const accessToken = this.encryption.decrypt(customer.AccessToken) || customer.AccessToken;
                    const refreshToken = this.encryption.decrypt(customer.RefreshToken) || customer.RefreshToken;
                    
                    // Create Google service
                    const googleService = new GoogleContactsService(
                        accessToken,
                        refreshToken,
                        this.config.google.clientId,
                        this.config.google.clientSecret
                    );
                    
                    // Get contact count
                    const contactCount = await googleService.getContactCount();
                    
                    if (contactCount !== null) {
                        await this.db.updateCustomerContactCount(customer.Phone, contactCount);
                        updated++;
                        
                        // Check if over limit
                        if (contactCount >= 25000) {
                            await this.sendErrorNotification(
                                customer.Phone,
                                'CONTACT_LIMIT_MAX',
                                `החשבון מכיל ${contactCount} אנשי קשר - חריגה ממגבלת 25,000`,
                                customer.FullName
                            );
                        }
                    }
                    
                    // Small delay to avoid rate limits
                    await new Promise(r => setTimeout(r, 300));
                    
                } catch (err) {
                    errors++;
                    // Only log if it's not a known token error
                    if (!err.message?.includes('TOKEN') && !err.message?.includes('401')) {
                        console.log(`Count update error for ${customer.Phone}: ${err.message}`);
                    }
                }
            }
            
            console.log(`Contact count update: ${updated} updated, ${errors} errors`);
            
        } finally {
            connection.release();
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
