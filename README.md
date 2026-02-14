# Registration Form - Botomat

טופס הרשמה עם Google OAuth, שמירה ל-MySQL ושליחת הודעה לוואטסאפ.

## Features

- טופס הרשמה (שם פרטי, משפחה, טלפון, אימייל)
- התחברות עם Google OAuth
- הרשאות לגישה לאנשי קשר
- שמירת טוקנים ב-MySQL
- שליחת הודעה לוואטסאפ

## Local Development

```bash
# Install dependencies
npm install

# Copy env file and fill in values
cp .env.example .env

# Run dev server
npm run dev
```

Open http://localhost:3000

## Production Deployment (Docker)

```bash
# Create .env file
cp .env.example .env
# Edit .env with production values

# Build and run
docker-compose up -d --build
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `PORT` | Server port (default: 3000) |
| `BASE_URL` | Public URL of the site |
| `GOOGLE_CLIENT_ID` | Google OAuth client ID |
| `GOOGLE_CLIENT_SECRET` | Google OAuth client secret |
| `GOOGLE_REDIRECT_URI` | OAuth callback URL |
| `MYSQL_HOST` | MySQL host |
| `MYSQL_PORT` | MySQL port (default: 3306) |
| `MYSQL_USER` | MySQL username |
| `MYSQL_PASSWORD` | MySQL password |
| `MYSQL_DATABASE` | MySQL database name |
| `WHATSAPP_API_URL` | WhatsApp API endpoint |
| `WHATSAPP_API_KEY` | WhatsApp API key |
| `WHATSAPP_SESSION` | WhatsApp session ID |
| `WHATSAPP_CHAT_ID` | WhatsApp chat/group ID |

## Google Cloud Console Setup

Add to Authorized redirect URIs:
- `https://registration.botomat.co.il/callback`
- `http://localhost:3000/callback` (for local dev)

## Database Table

```sql
CREATE TABLE IF NOT EXISTS לקוחות (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Email VARCHAR(255) UNIQUE,
    Phone VARCHAR(20),
    Password VARCHAR(10),
    FullName VARCHAR(255),
    Project VARCHAR(255),
    AccessToken TEXT,
    RefreshToken TEXT,
    ExpirationTime INT,
    Hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```
