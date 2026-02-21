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

    return router;
};
