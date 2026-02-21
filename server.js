require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const mysql = require('mysql2/promise');
const path = require('path');
const cookieParser = require('cookie-parser');

// Services
const ContactSaverService = require('./src/services/contactSaver');
const EncryptionService = require('./src/services/encryption');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

// MySQL Connection Pool
const pool = mysql.createPool({
    host: process.env.MYSQL_HOST,
    port: process.env.MYSQL_PORT || 3306,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// Config
const config = {
    baseUrl: process.env.BASE_URL || 'http://localhost:3000',
    encryptionKey: process.env.ENCRYPTION_KEY || 'default-encryption-key-change-in-production',
    jwtSecret: process.env.JWT_SECRET || 'jwt-secret-change-in-production',
    database: process.env.MYSQL_DATABASE,
    google: {
        clientId: process.env.GOOGLE_CLIENT_ID,
        clientSecret: process.env.GOOGLE_CLIENT_SECRET,
        redirectUri: process.env.GOOGLE_REDIRECT_URI || `${process.env.BASE_URL}/callback`
    },
    whatsapp: {
        apiUrl: process.env.WHATSAPP_API_URL,
        apiKey: process.env.WHATSAPP_API_KEY,
        session: process.env.WHATSAPP_SESSION,
        chatId: process.env.WHATSAPP_CHAT_ID
    }
};

// Initialize encryption service
const encryption = new EncryptionService(config.encryptionKey);

// Initialize Contact Saver Service
const contactSaver = new ContactSaverService(pool, config);

// Admin routes
const adminRoutes = require('./src/routes/admin')(pool, config, contactSaver);
app.use('/admin', adminRoutes);

// Admin page
app.get('/admin', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin', 'index.html'));
});

// Home page
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Register page (alias for home)
app.get('/register', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Get config for frontend
app.get('/api/config', (req, res) => {
    res.json({
        clientId: config.google.clientId,
        redirectUri: config.google.redirectUri
    });
});

// Notify on registration start
app.post('/api/notify-start', async (req, res) => {
    try {
        const { firstName, lastName, phone, email } = req.body;
        
        const cleanPhone = phone.replace(/\s+/g, '').replace(/^\+/, '').replace(/^0/, '972');
        const whatsappPhone = cleanPhone.startsWith('972') ? cleanPhone : '972' + cleanPhone;
        
        const message = `*בעל עסק התחיל הרשמה כעת*

*שם:* ${firstName} ${lastName}
*טלפון:* ${phone}
*אימייל:* ${email}

*קישור לווצאפ:*
wa.me/${whatsappPhone}`;

        await axios.post(config.whatsapp.apiUrl, {
            chatId: config.whatsapp.chatId,
            text: message,
            session: config.whatsapp.session
        }, {
            headers: {
                'accept': 'application/json',
                'X-Api-Key': config.whatsapp.apiKey,
                'Content-Type': 'application/json'
            }
        });

        res.json({ success: true });
    } catch (error) {
        console.error('Notify start error:', error.response?.data || error.message);
        res.json({ success: false });
    }
});

// OAuth Callback
app.get('/callback', async (req, res) => {
    const { code, state, error } = req.query;

    if (error) {
        return res.redirect(`/result.html?error=${encodeURIComponent(error)}`);
    }

    if (!code) {
        return res.redirect('/result.html?error=no_code');
    }

    try {
        // Parse state to get user data (decode UTF-8 for Hebrew support)
        let userData = {};
        if (state) {
            try {
                const decoded = Buffer.from(state, 'base64').toString('utf-8');
                userData = JSON.parse(decodeURIComponent(escape(decoded)));
            } catch (e) {
                try {
                    userData = JSON.parse(Buffer.from(state, 'base64').toString());
                } catch (e2) {
                    console.error('Error parsing state:', e2);
                }
            }
        }

        // Exchange code for tokens
        const tokenResponse = await axios.post('https://oauth2.googleapis.com/token', {
            client_id: config.google.clientId,
            client_secret: config.google.clientSecret,
            code: code,
            grant_type: 'authorization_code',
            redirect_uri: config.google.redirectUri
        });

        const { access_token, refresh_token, expires_in } = tokenResponse.data;

        // Get user email from Google
        const userInfoResponse = await axios.get('https://www.googleapis.com/oauth2/v2/userinfo', {
            headers: { Authorization: `Bearer ${access_token}` }
        });

        const googleEmail = userInfoResponse.data.email;
        const email = userData.email || googleEmail;
        const phone = userData.phone || '';
        const firstName = userData.firstName || '';
        const lastName = userData.lastName || '';
        const fullName = `${firstName} ${lastName}`.trim();

        // Format phone number
        const cleanPhone = phone.replace(/[^0-9]/g, '');
        const formattedPhone = cleanPhone.length >= 9 
            ? '972' + cleanPhone.slice(-9) 
            : cleanPhone;
        const password = cleanPhone.slice(-6);

        // Encrypt tokens before saving
        const encryptedAccessToken = encryption.encrypt(access_token);
        const encryptedRefreshToken = encryption.encrypt(refresh_token);

        // Save to database
        const connection = await pool.getConnection();
        try {
            await connection.execute(`
                INSERT INTO לקוחות (Email, Phone, Password, AccessToken, RefreshToken, ExpirationTime, Hash, FullName)
                VALUES (?, ?, ?, ?, ?, ?, SHA2(CONCAT(?, ?), 256), ?)
                ON DUPLICATE KEY UPDATE
                    AccessToken = VALUES(AccessToken),
                    RefreshToken = VALUES(RefreshToken),
                    ExpirationTime = VALUES(ExpirationTime),
                    Hash = VALUES(Hash),
                    FullName = COALESCE(NULLIF(VALUES(FullName), ''), FullName),
                    Phone = COALESCE(NULLIF(VALUES(Phone), ''), Phone)
            `, [email, formattedPhone, password, encryptedAccessToken, encryptedRefreshToken, expires_in, email, formattedPhone, fullName]);

            // Get user data for WhatsApp message
            const [rows] = await connection.execute(`
                SELECT FullName, Phone, Project, Email, AccessToken, RefreshToken
                FROM לקוחות
                WHERE Email = ?
                LIMIT 1
            `, [email]);

            const user = rows[0] || { FullName: fullName, Phone: formattedPhone, Email: email };

            // Send WhatsApp notification
            await sendWhatsAppNotification(user);

            connection.release();

            // Redirect to success page
            const resultParams = new URLSearchParams({
                success: 'true',
                email: email,
                name: fullName
            });
            res.redirect(`/result.html?${resultParams.toString()}`);

        } catch (dbError) {
            connection.release();
            console.error('Database error:', dbError);
            res.redirect(`/result.html?error=database_error&message=${encodeURIComponent(dbError.message)}`);
        }

    } catch (error) {
        console.error('OAuth error:', error.response?.data || error.message);
        const errorMsg = error.response?.data?.error_description || error.response?.data?.error || error.message || 'Unknown error';
        res.redirect(`/result.html?error=oauth_error&message=${encodeURIComponent(errorMsg)}`);
    }
});

// Send WhatsApp notification
async function sendWhatsAppNotification(user) {
    try {
        const whatsappPhone = user.Phone?.replace(/\s+/g, '').replace(/^\+/, '').replace(/^0/, '972');
        
        const message = `*טוקן חדש נוסף כעת*

*שם:* ${user.FullName || 'לא צוין'}
*טלפון:* ${user.Phone || 'לא צוין'}
*אימייל:* ${user.Email}
*טוקן:* ${user.AccessToken ? 'קיים' : 'לא קיים'}
*ריפרש:* ${user.RefreshToken ? 'קיים' : 'לא קיים'}

*קישור לווצאפ:*
wa.me/${whatsappPhone}

*קישור להוספת פרוייקט ללקוח:*
https://neriyabudraham.co.il/add`;

        await axios.post(config.whatsapp.apiUrl, {
            chatId: config.whatsapp.chatId,
            text: message,
            session: config.whatsapp.session
        }, {
            headers: {
                'accept': 'application/json',
                'X-Api-Key': config.whatsapp.apiKey,
                'Content-Type': 'application/json'
            }
        });

    } catch (error) {
        console.error('WhatsApp notification error:', error.response?.data || error.message);
    }
}

// Health check
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
    
    // Start contact saver scheduler - runs every minute
    console.log('Starting contact saver scheduler (every 60 seconds)...');
    setInterval(() => {
        contactSaver.run().catch(err => console.error('Contact saver error:', err));
    }, 60 * 1000);
    
    // Run immediately on startup (after 10 seconds to let everything initialize)
    setTimeout(() => {
        contactSaver.run().catch(err => console.error('Initial contact saver error:', err));
    }, 10000);
});
