// Configuration - will be loaded from server
let CONFIG = {
    clientId: '',
    redirectUri: '',
    scopes: [
        'https://www.googleapis.com/auth/contacts',
        'https://www.googleapis.com/auth/script.external_request',
        'https://www.googleapis.com/auth/userinfo.email'
    ].join(' ')
};

// Load config from server
async function loadConfig() {
    try {
        const response = await fetch('/api/config');
        const data = await response.json();
        CONFIG.clientId = data.clientId;
        CONFIG.redirectUri = data.redirectUri;
    } catch (error) {
        console.error('Failed to load config:', error);
        // Fallback to hardcoded values
        CONFIG.clientId = '335567162380-4fddiii2ogdvok371r36vd3f3b4t55au.apps.googleusercontent.com';
        CONFIG.redirectUri = window.location.origin + '/callback';
    }
}

// DOM Elements
const form = document.getElementById('registrationForm');
const googleBtn = document.getElementById('googleBtn');
const inputs = form.querySelectorAll('input');

// Form validation state
let isFormValid = false;

// Validate form
function validateForm() {
    isFormValid = form.checkValidity();
    googleBtn.disabled = !isFormValid;
    return isFormValid;
}

// Add input listeners
inputs.forEach(input => {
    input.addEventListener('input', validateForm);
    input.addEventListener('blur', () => {
        if (!input.validity.valid && input.value) {
            input.classList.add('shake');
            setTimeout(() => input.classList.remove('shake'), 300);
        }
    });
});

// Initial validation
validateForm();

// Handle form submission
form.addEventListener('submit', async (e) => {
    e.preventDefault();
    
    if (!validateForm()) {
        return;
    }

    // Make sure config is loaded
    if (!CONFIG.clientId) {
        await loadConfig();
    }
    
    // Collect form data
    const formData = {
        firstName: document.getElementById('firstName').value.trim(),
        lastName: document.getElementById('lastName').value.trim(),
        phone: document.getElementById('phone').value.trim(),
        email: document.getElementById('email').value.trim(),
        timestamp: new Date().toISOString()
    };
    
    // Send notification that registration started
    try {
        await fetch('/api/notify-start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(formData)
        });
    } catch (e) {
        console.log('Notify error:', e);
    }
    
    // Build Google OAuth URL with state parameter (encode UTF-8 for Hebrew support)
    const state = btoa(unescape(encodeURIComponent(JSON.stringify(formData))));
    
    const authUrl = new URL('https://accounts.google.com/o/oauth2/auth');
    authUrl.searchParams.set('client_id', CONFIG.clientId);
    authUrl.searchParams.set('redirect_uri', CONFIG.redirectUri);
    authUrl.searchParams.set('response_type', 'code');
    authUrl.searchParams.set('scope', CONFIG.scopes);
    authUrl.searchParams.set('access_type', 'offline');
    authUrl.searchParams.set('prompt', 'consent');
    authUrl.searchParams.set('state', state);
    
    // Redirect to Google OAuth
    window.location.href = authUrl.toString();
});

// Phone number formatting
document.getElementById('phone').addEventListener('input', function(e) {
    let value = e.target.value.replace(/[^\d]/g, '');
    
    // Format as Israeli phone number
    if (value.length > 3 && value.length <= 7) {
        value = value.slice(0, 3) + '-' + value.slice(3);
    } else if (value.length > 7) {
        value = value.slice(0, 3) + '-' + value.slice(3, 10);
    }
    
    e.target.value = value;
});

// Load config on page load
loadConfig();
