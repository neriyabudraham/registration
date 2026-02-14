# Registration Form - Botomat

טופס הרשמה עם התחברות Google OAuth לגישה לאנשי קשר.

## Features

- טופס הרשמה מודרני (שם פרטי, משפחה, טלפון, אימייל)
- התחברות עם Google OAuth
- הרשאות לגישה לאנשי קשר
- דף callback להצגת תוצאה (הצלחה/כישלון)

## Deployment

### Docker

```bash
# Build and run
docker-compose up -d --build

# Or manually
docker build -t registration-botomat .
docker run -d -p 3080:80 --name registration-botomat registration-botomat
```

### Server Setup

הדומיין: https://registration.botomat.co.il/  
מיקום בשרת: `/www/wwwroot/registration.botomat.co.il`

## Google OAuth Configuration

- Client ID: `335567162380-01vu2ekj253hhltsg1lfc2g6vh72jq40.apps.googleusercontent.com`
- Redirect URI: `https://n8n.neriyabudraham.co.il/webhook/callback`
- Scopes:
  - `https://www.googleapis.com/auth/contacts`
  - `https://www.googleapis.com/auth/script.external_request`
  - `https://www.googleapis.com/auth/userinfo.email`

## Flow

1. משתמש ממלא טופס
2. לחיצה על "התחבר עם Google"
3. הפניה ל-Google OAuth
4. Google מפנה ל-n8n webhook עם code
5. n8n מטפל באימות ומחזיר לדף callback עם תוצאה
