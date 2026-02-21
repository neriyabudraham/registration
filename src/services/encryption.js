const crypto = require('crypto');

const ALGORITHM = 'aes-256-gcm';
const IV_LENGTH = 16;
const SALT_LENGTH = 64;
const TAG_LENGTH = 16;
const KEY_LENGTH = 32;
const ITERATIONS = 100000;

class EncryptionService {
    constructor(secretKey) {
        this.secretKey = secretKey || process.env.ENCRYPTION_KEY || 'default-secret-key-change-me-in-production';
    }

    // Derive key from password using PBKDF2
    deriveKey(salt) {
        return crypto.pbkdf2Sync(this.secretKey, salt, ITERATIONS, KEY_LENGTH, 'sha256');
    }

    // Encrypt text
    encrypt(text) {
        if (!text) return null;
        
        try {
            const salt = crypto.randomBytes(SALT_LENGTH);
            const iv = crypto.randomBytes(IV_LENGTH);
            const key = this.deriveKey(salt);
            
            const cipher = crypto.createCipheriv(ALGORITHM, key, iv);
            const encrypted = Buffer.concat([cipher.update(text, 'utf8'), cipher.final()]);
            const tag = cipher.getAuthTag();
            
            // Combine: salt + iv + tag + encrypted
            const result = Buffer.concat([salt, iv, tag, encrypted]);
            return result.toString('base64');
        } catch (error) {
            console.error('Encryption error:', error);
            return null;
        }
    }

    // Decrypt text
    decrypt(encryptedText) {
        if (!encryptedText) return null;
        
        try {
            const buffer = Buffer.from(encryptedText, 'base64');
            
            const salt = buffer.slice(0, SALT_LENGTH);
            const iv = buffer.slice(SALT_LENGTH, SALT_LENGTH + IV_LENGTH);
            const tag = buffer.slice(SALT_LENGTH + IV_LENGTH, SALT_LENGTH + IV_LENGTH + TAG_LENGTH);
            const encrypted = buffer.slice(SALT_LENGTH + IV_LENGTH + TAG_LENGTH);
            
            const key = this.deriveKey(salt);
            
            const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);
            decipher.setAuthTag(tag);
            
            const decrypted = Buffer.concat([decipher.update(encrypted), decipher.final()]);
            return decrypted.toString('utf8');
        } catch (error) {
            console.error('Decryption error:', error);
            return null;
        }
    }

    // Hash password (one-way)
    hashPassword(password) {
        const salt = crypto.randomBytes(16).toString('hex');
        const hash = crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha512').toString('hex');
        return `${salt}:${hash}`;
    }

    // Verify password
    verifyPassword(password, storedHash) {
        const [salt, hash] = storedHash.split(':');
        const verifyHash = crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha512').toString('hex');
        return hash === verifyHash;
    }
}

module.exports = EncryptionService;
