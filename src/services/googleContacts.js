const axios = require('axios');

class GoogleContactsService {
    constructor(accessToken, refreshToken, clientId, clientSecret) {
        this.accessToken = accessToken;
        this.refreshToken = refreshToken;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.baseUrl = 'https://people.googleapis.com/v1';
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
            // Normalize phone for search
            const normalizedPhone = phone.replace(/[^0-9]/g, '');
            
            const response = await this.makeRequest(
                'GET',
                `${this.baseUrl}/people:searchContacts?query=${normalizedPhone}&readMask=names,phoneNumbers&pageSize=10`
            );
            
            const results = response.data.results || [];
            
            // Check if any result matches the phone number
            for (const result of results) {
                const person = result.person;
                if (person.phoneNumbers) {
                    for (const phoneObj of person.phoneNumbers) {
                        const contactPhone = phoneObj.value.replace(/[^0-9]/g, '');
                        if (contactPhone.includes(normalizedPhone) || normalizedPhone.includes(contactPhone)) {
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
    async saveContactWithLabel(name, phone, labelName) {
        try {
            // Format label name (remove underscores)
            const formattedLabelName = this.formatLabelName(labelName);
            
            // 1. Get or create the label
            const group = await this.getOrCreateContactGroup(formattedLabelName);
            const groupResourceName = group.resourceName;
            
            // 2. Check if contact already exists
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
            
            // 3. Create new contact with group
            const newContact = await this.createContact(name, phone, groupResourceName);
            return { status: 'created', contact: newContact };
            
        } catch (error) {
            // Check for rate limit
            if (error.response?.status === 429) {
                const retryAfter = error.response.headers['retry-after'] || 60;
                throw { code: 'RATE_LIMIT', retryAfter: parseInt(retryAfter) };
            }
            
            // Check for permission issues
            if (error.response?.status === 403) {
                throw { code: 'PERMISSION_DENIED', message: error.response?.data?.error?.message };
            }
            
            // Check for invalid token
            if (error.message === 'TOKEN_REFRESH_FAILED') {
                throw { code: 'TOKEN_INVALID', message: 'Unable to refresh access token' };
            }
            
            throw { code: 'UNKNOWN_ERROR', message: error.message };
        }
    }
}

module.exports = GoogleContactsService;
