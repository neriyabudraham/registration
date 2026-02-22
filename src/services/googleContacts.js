const axios = require('axios');

class GoogleContactsService {
    constructor(accessToken, refreshToken, clientId, clientSecret) {
        this.accessToken = accessToken;
        this.refreshToken = refreshToken;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.baseUrl = 'https://people.googleapis.com/v1';
    }

    // ==================== NAME SANITIZATION ====================
    
    // Characters to completely remove
    static CHARS_TO_REMOVE = [
        '=', '*', ',', '•', '°', '®️', '>', '<', '~', '@', '#', '$', '%', '^', '₪',
        '{', '}', '[', ']', '+', '◇', '★', '☆', '♡', '⚜️', '⚡️', '🇮🇱', '❤️', '😜',
        '💖', '✈️', '✌', '✨', '❤', '❤️‍🔥', '💛', '🔥', '🃏', '🌊', '⚔️', '🖤', '🌗',
        '👻', '🌱', '🌷', '🌸', '🌼', '🌹', '?', '🌿', '🍁', '🍏', '🍒', '🍦', '🤍',
        '🍷', '🎀', '🎗️', '🤔', '🎬', '🏍', '🏡', '💚', '😄', '👾', '👑', '&', '!',
        '|', '_', ';', '`', '\\', '\t'
    ];

    // Get date suffix MM/YY
    getDateSuffix() {
        const now = new Date();
        const month = String(now.getMonth() + 1).padStart(2, '0');
        const year = String(now.getFullYear()).slice(-2);
        return `${month}/${year}`;
    }

    // Sanitize contact name with all rules and add date suffix
    sanitizeName(name, defaultName = 'צופה') {
        const dateSuffix = this.getDateSuffix();
        
        if (!name || typeof name !== 'string') {
            return `${defaultName} ${dateSuffix}`;
        }

        let cleaned = name;

        // Replace newlines with space
        cleaned = cleaned.replace(/[\r\n]+/g, ' ');

        // Remove all special characters and emojis
        for (const char of GoogleContactsService.CHARS_TO_REMOVE) {
            cleaned = cleaned.split(char).join('');
        }

        // Remove -me suffix
        cleaned = cleaned.replace(/-me$/gi, '');

        // Remove leading numbers
        cleaned = cleaned.replace(/^[0-9]+/, '');

        // Remove leading special chars: . - _ ״ " ' space
        cleaned = cleaned.replace(/^[\.\-_״"'\s]+/, '');

        // Remove trailing special chars: . - _ ״ " ' | ! space
        cleaned = cleaned.replace(/[\.\-_״"'\s\|!]+$/, '');

        // Remove parentheses (we'll add our own for duplicates)
        cleaned = cleaned.replace(/[()]/g, '');

        // Replace double spaces with single
        cleaned = cleaned.replace(/\s{2,}/g, ' ');

        // Replace double special chars (.. -- etc)
        cleaned = cleaned.replace(/([.\-]){2,}/g, '$1');

        // Trim
        cleaned = cleaned.trim();

        // Max 35 characters (leave room for date suffix)
        if (cleaned.length > 35) {
            cleaned = cleaned.substring(0, 35).trim();
        }

        // Validate: must have at least 2 consecutive Hebrew or English letters
        const hasValidChars = /[a-zA-Zא-ת]{2,}/.test(cleaned);
        if (!hasValidChars || cleaned.length < 2) {
            return `${defaultName} ${dateSuffix}`;
        }

        // Add date suffix to the name
        return `${cleaned} ${dateSuffix}`;
    }

    // Generate default name with date suffix
    generateDefaultName(baseName = 'צופה') {
        const now = new Date();
        const month = String(now.getMonth() + 1).padStart(2, '0');
        const year = String(now.getFullYear()).slice(-2);
        return `${baseName} ${month}/${year}`;
    }

    // Refresh access token if needed
    async refreshAccessToken() {
        try {
            const response = await axios.post('https://oauth2.googleapis.com/token', {
                client_id: this.clientId,
                client_secret: this.clientSecret,
                refresh_token: this.refreshToken,
                grant_type: 'refresh_token'
            });
            
            this.accessToken = response.data.access_token;
            return {
                accessToken: response.data.access_token,
                expiresIn: response.data.expires_in
            };
        } catch (error) {
            console.error('Token refresh error:', error.response?.data || error.message);
            throw new Error('TOKEN_REFRESH_FAILED');
        }
    }

    // Make authenticated request with auto-retry on 401
    async makeRequest(method, url, data = null, retried = false) {
        try {
            const config = {
                method,
                url,
                headers: {
                    'Authorization': `Bearer ${this.accessToken}`,
                    'Content-Type': 'application/json'
                }
            };
            
            if (data) {
                config.data = data;
            }
            
            return await axios(config);
        } catch (error) {
            // If 401 and not retried, refresh token and retry
            if (error.response?.status === 401 && !retried) {
                await this.refreshAccessToken();
                return this.makeRequest(method, url, data, true);
            }
            throw error;
        }
    }

    // Get all contact groups (labels)
    async getContactGroups() {
        try {
            const response = await this.makeRequest(
                'GET',
                `${this.baseUrl}/contactGroups?pageSize=1000`
            );
            return response.data.contactGroups || [];
        } catch (error) {
            console.error('Get contact groups error:', error.response?.data || error.message);
            throw error;
        }
    }

    // Create a new contact group (label)
    async createContactGroup(name) {
        try {
            const response = await this.makeRequest(
                'POST',
                `${this.baseUrl}/contactGroups`,
                {
                    contactGroup: {
                        name: name
                    }
                }
            );
            return response.data;
        } catch (error) {
            console.error('Create contact group error:', error.response?.data || error.message);
            throw error;
        }
    }

    // Get or create contact group by name
    async getOrCreateContactGroup(name) {
        const groups = await this.getContactGroups();
        
        // Find existing group
        const existingGroup = groups.find(g => g.name === name || g.formattedName === name);
        if (existingGroup) {
            return existingGroup;
        }
        
        // Create new group
        return await this.createContactGroup(name);
    }

    // Search for contact by phone number
    async searchContactByPhone(phone) {
        try {
            // Normalize phone for search - try multiple formats
            const normalizedPhone = phone.replace(/[^0-9]/g, '');
            
            // Try full number first
            let response = await this.makeRequest(
                'GET',
                `${this.baseUrl}/people:searchContacts?query=${normalizedPhone}&readMask=names,phoneNumbers&pageSize=30`
            );
            
            let results = response.data.results || [];
            
            // Also try with last 9 digits (Israeli format without country code)
            if (results.length === 0 && normalizedPhone.length > 9) {
                const shortPhone = normalizedPhone.slice(-9);
                response = await this.makeRequest(
                    'GET',
                    `${this.baseUrl}/people:searchContacts?query=${shortPhone}&readMask=names,phoneNumbers&pageSize=30`
                );
                results = response.data.results || [];
            }
            
            // Check if any result matches the phone number
            for (const result of results) {
                const person = result.person;
                if (person.phoneNumbers) {
                    for (const phoneObj of person.phoneNumbers) {
                        const contactPhone = phoneObj.value.replace(/[^0-9]/g, '');
                        // Compare last 9 digits
                        const last9Contact = contactPhone.slice(-9);
                        const last9Search = normalizedPhone.slice(-9);
                        if (last9Contact === last9Search) {
                            return person;
                        }
                    }
                }
            }
            
            return null;
        } catch (error) {
            // 404 means no results
            if (error.response?.status === 404) {
                return null;
            }
            console.error('Search contact error:', error.response?.data || error.message);
            throw error;
        }
    }

    // Search for contacts by name to check for duplicates
    async searchContactsByName(name) {
        try {
            const response = await this.makeRequest(
                'GET',
                `${this.baseUrl}/people:searchContacts?query=${encodeURIComponent(name)}&readMask=names&pageSize=100`
            );
            
            const results = response.data.results || [];
            const names = [];
            
            for (const result of results) {
                const person = result.person;
                if (person.names) {
                    for (const nameObj of person.names) {
                        if (nameObj.displayName) names.push(nameObj.displayName);
                        if (nameObj.givenName) names.push(nameObj.givenName);
                    }
                }
            }
            
            return names;
        } catch (error) {
            if (error.response?.status === 404) {
                return [];
            }
            console.error('Search contacts by name error:', error.response?.data || error.message);
            return []; // Return empty on error, don't block saving
        }
    }

    // Generate unique name by checking existing contacts
    async generateUniqueName(baseName) {
        // Search for existing contacts with similar name
        const existingNames = await this.searchContactsByName(baseName);
        
        // Check if exact name exists
        if (!existingNames.some(n => n === baseName)) {
            return baseName;
        }
        
        // Find next available number
        let counter = 1;
        let uniqueName = `${baseName} (${counter})`;
        
        while (existingNames.some(n => n === uniqueName)) {
            counter++;
            uniqueName = `${baseName} (${counter})`;
            
            // Safety limit
            if (counter > 1000) break;
        }
        
        return uniqueName;
    }

    // Create a new contact
    async createContact(name, phone, groupResourceName = null) {
        try {
            const contactData = {
                names: [{
                    givenName: name,
                    displayName: name
                }],
                phoneNumbers: [{
                    value: phone,
                    type: 'mobile'
                }]
            };
            
            // Add to group if specified
            if (groupResourceName) {
                contactData.memberships = [{
                    contactGroupMembership: {
                        contactGroupResourceName: groupResourceName
                    }
                }];
            }
            
            const response = await this.makeRequest(
                'POST',
                `${this.baseUrl}/people:createContact?personFields=names,phoneNumbers,memberships`,
                contactData
            );
            
            return response.data;
        } catch (error) {
            console.error('Create contact error:', error.response?.data || error.message);
            throw error;
        }
    }

    // Add contact to group
    async addContactToGroup(contactResourceName, groupResourceName) {
        try {
            const response = await this.makeRequest(
                'POST',
                `${this.baseUrl}/${groupResourceName}/members:modify`,
                {
                    resourceNamesToAdd: [contactResourceName]
                }
            );
            return response.data;
        } catch (error) {
            console.error('Add to group error:', error.response?.data || error.message);
            throw error;
        }
    }

    // Format label name (replace _ with space) and add date suffix MM/YY
    formatLabelName(name) {
        if (!name) return name;
        const formatted = name.replace(/_/g, ' ');
        const now = new Date();
        const month = String(now.getMonth() + 1).padStart(2, '0');
        const year = String(now.getFullYear()).slice(-2);
        return `${formatted} ${month}/${year}`;
    }

    // Main function: Save contact with label
    // defaultName: optional default name for invalid names (e.g., "צופה")
    async saveContactWithLabel(name, phone, labelName, defaultName = 'צופה') {
        try {
            // Format label name (remove underscores, add date)
            const formattedLabelName = this.formatLabelName(labelName);
            
            // 1. Get or create the label
            const group = await this.getOrCreateContactGroup(formattedLabelName);
            const groupResourceName = group.resourceName;
            
            // 2. Check if contact already exists by phone
            const existingContact = await this.searchContactByPhone(phone);
            
            if (existingContact) {
                // Contact exists - add to group if not already in it
                const isMember = existingContact.memberships?.some(
                    m => m.contactGroupMembership?.contactGroupResourceName === groupResourceName
                );
                
                if (!isMember) {
                    await this.addContactToGroup(existingContact.resourceName, groupResourceName);
                }
                
                return { status: 'existed', contact: existingContact };
            }
            
            // 3. Sanitize the name
            let sanitizedName = this.sanitizeName(name, defaultName);
            
            // 4. Generate unique name (avoid duplicates)
            const uniqueName = await this.generateUniqueName(sanitizedName);
            
            // 5. Create new contact with group
            const newContact = await this.createContact(uniqueName, phone, groupResourceName);
            return { status: 'created', contact: newContact, savedName: uniqueName };
            
        } catch (error) {
            const errorData = error.response?.data?.error;
            const errorMessage = errorData?.message || error.message;
            const errorStatus = errorData?.status || error.response?.status;
            
            // Check for rate limit (429)
            if (error.response?.status === 429) {
                const retryAfter = error.response.headers['retry-after'] || 60;
                throw { 
                    code: 'RATE_LIMIT_TEMPORARY', 
                    message: `מגבלת קצב זמנית - המתן ${retryAfter} שניות`,
                    retryAfter: parseInt(retryAfter) 
                };
            }
            
            // Check for quota exceeded (usually 403 with specific message)
            if (error.response?.status === 403) {
                // Check if it's a contact limit issue
                if (errorMessage?.includes('quota') || errorMessage?.includes('limit') || errorMessage?.includes('RESOURCE_EXHAUSTED')) {
                    throw { 
                        code: 'CONTACT_LIMIT_EXCEEDED', 
                        message: 'החשבון הגיע למגבלת אנשי קשר - יש למחוק אנשי קשר ישנים'
                    };
                }
                throw { 
                    code: 'PERMISSION_DENIED', 
                    message: errorMessage || 'אין הרשאה לגשת לאנשי הקשר'
                };
            }
            
            // Check for invalid token
            if (error.message === 'TOKEN_REFRESH_FAILED') {
                throw { 
                    code: 'TOKEN_INVALID', 
                    message: 'לא ניתן לרענן את הטוקן - יש להתחבר מחדש'
                };
            }
            
            // Check for 400 errors (bad request - often quota)
            if (error.response?.status === 400) {
                if (errorMessage?.includes('quota') || errorMessage?.includes('limit')) {
                    throw { 
                        code: 'CONTACT_LIMIT_EXCEEDED', 
                        message: 'החשבון הגיע למגבלת אנשי קשר'
                    };
                }
            }
            
            throw { 
                code: 'UNKNOWN_ERROR', 
                message: errorMessage || error.message || 'שגיאה לא ידועה'
            };
        }
    }

    // Get total contact count in the account
    async getContactCount() {
        try {
            const response = await this.makeRequest(
                'GET',
                `${this.baseUrl}/people/me/connections?pageSize=1&personFields=names`
            );
            
            // The totalPeople field gives us the count
            return response.data.totalPeople || response.data.totalItems || 0;
        } catch (error) {
            console.error('Get contact count error:', error.response?.data || error.message);
            return null; // Return null on error, not 0
        }
    }
}

module.exports = GoogleContactsService;
