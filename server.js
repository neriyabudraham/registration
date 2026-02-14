require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const mysql = require('mysql2/promise');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
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
        // Parse state to get user data
        let userData = {};
        if (state) {
            try {
                userData = JSON.parse(Buffer.from(state, 'base64').toString());
            } catch (e) {
                console.error('Error parsing state:', e);
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

        // Save to database
        const connection = await pool.getConnection();
        try {
            // Update if exists, insert if not
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
            `, [email, formattedPhone, password, access_token, refresh_token, expires_in, email, formattedPhone, fullName]);

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
        res.redirect(`/result.html?error=oauth_error&message=${encodeURIComponent(error.message)}`);
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

        console.log('WhatsApp notification sent successfully');
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
});
