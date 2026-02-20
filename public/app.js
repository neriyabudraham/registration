// Configuration
let CONFIG = {
    clientId: '',
    redirectUri: '',
    scopes: [
        'https://www.googleapis.com/auth/contacts',
        'https://www.googleapis.com/auth/script.external_request',
        'https://www.googleapis.com/auth/userinfo.email'
    ].join(' ')
};

// Form data storage
let formData = {
    firstName: '',
    lastName: '',
    phone: '',
    email: ''
};

// DOM Elements
const instructionsSection = document.getElementById('instructionsSection');
const formSection = document.getElementById('formSection');
const videoModal = document.getElementById('videoModal');
const registrationForm = document.getElementById('registrationForm');

// Load config from server
async function loadConfig() {
    try {
        const response = await fetch('/api/config');
        const data = await response.json();
        CONFIG.clientId = data.clientId;
        CONFIG.redirectUri = data.redirectUri;
    } catch (error) {
        console.error('Failed to load config:', error);
        CONFIG.clientId = '335567162380-4fddiii2ogdvok371r36vd3f3b4t55au.apps.googleusercontent.com';
        CONFIG.redirectUri = window.location.origin + '/callback';
    }
}

// Transition helper
function transitionSections(hideSection, showSection) {
    hideSection.classList.add('fade-out');
    
    setTimeout(() => {
        hideSection.style.display = 'none';
        hideSection.classList.remove('fade-out');
        
        showSection.style.display = 'block';
        showSection.classList.add('fade-in');
        
        setTimeout(() => {
            showSection.classList.remove('fade-in');
        }, 400);
    }, 400);
}

// Instructions checkbox handler
document.getElementById('readInstructions').addEventListener('change', function() {
    if (this.checked) {
        showFormSection();
    }
});

function showFormSection() {
    // Restore saved values
    document.getElementById('firstName').value = formData.firstName;
    document.getElementById('lastName').value = formData.lastName;
    document.getElementById('phone').value = formData.phone;
    document.getElementById('email').value = formData.email;
    
    transitionSections(instructionsSection, formSection);
}

// Back button handler
document.getElementById('backToInstructions').addEventListener('click', function() {
    // Save current values
    formData.firstName = document.getElementById('firstName').value.trim();
    formData.lastName = document.getElementById('lastName').value.trim();
    formData.phone = document.getElementById('phone').value.trim();
    formData.email = document.getElementById('email').value.trim();
    
    transitionSections(formSection, instructionsSection);
    
    // Reset checkbox
    document.getElementById('readInstructions').checked = false;
});

// Video Modal handlers
document.getElementById('openVideoBtn').addEventListener('click', function(e) {
    e.preventDefault();
    openVideoModal();
});

document.getElementById('closeVideoBtn').addEventListener('click', closeVideoModal);

document.getElementById('videoWatched').addEventListener('change', function() {
    document.getElementById('continueFromVideo').disabled = !this.checked;
});

document.getElementById('continueFromVideo').addEventListener('click', function() {
    closeVideoModal();
    showFormSection();
});

// Close modal on backdrop click
videoModal.addEventListener('click', function(e) {
    if (e.target === videoModal) {
        closeVideoModal();
    }
});

// Close modal on Escape key
document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape' && videoModal.classList.contains('active')) {
        closeVideoModal();
    }
});

function openVideoModal() {
    const video = document.getElementById('instructionVideo');
    video.currentTime = 0;
    videoModal.classList.add('active');
    document.body.style.overflow = 'hidden';
}

function closeVideoModal() {
    const video = document.getElementById('instructionVideo');
    video.pause();
    video.currentTime = 0;
    videoModal.classList.remove('active');
    document.body.style.overflow = '';
}

// Form submission
registrationForm.addEventListener('submit', async function(e) {
    e.preventDefault();
    
    const submitBtn = document.getElementById('submitBtn');
    const errorMessage = document.getElementById('errorMessage');
    
    // Collect form data
    formData = {
        firstName: document.getElementById('firstName').value.trim(),
        lastName: document.getElementById('lastName').value.trim(),
        phone: document.getElementById('phone').value.trim(),
        email: document.getElementById('email').value.trim()
    };
    
    // Validate
    if (!formData.firstName || !formData.lastName || !formData.phone || !formData.email) {
        errorMessage.textContent = 'נא למלא את כל השדות';
        errorMessage.classList.add('show');
        return;
    }
    
    // Hide error
    errorMessage.classList.remove('show');
    
    // Disable button and show loading
    submitBtn.disabled = true;
    const originalContent = submitBtn.innerHTML;
    submitBtn.innerHTML = '<div class="spinner"></div><span>שולח...</span>';
    
    try {
        // Make sure config is loaded
        if (!CONFIG.clientId) {
            await loadConfig();
        }
        
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
        
        // Build state with form data (UTF-8 safe encoding)
        const state = btoa(unescape(encodeURIComponent(JSON.stringify({
            ...formData,
            timestamp: new Date().toISOString()
        }))));
        
        // Build Google OAuth URL
        const authUrl = new URL('https://accounts.google.com/o/oauth2/auth');
        authUrl.searchParams.set('client_id', CONFIG.clientId);
        authUrl.searchParams.set('redirect_uri', CONFIG.redirectUri);
        authUrl.searchParams.set('response_type', 'code');
        authUrl.searchParams.set('scope', CONFIG.scopes);
        authUrl.searchParams.set('access_type', 'offline');
        authUrl.searchParams.set('prompt', 'consent');
        authUrl.searchParams.set('state', state);
        
        // Redirect to Google
        window.location.href = authUrl.toString();
        
    } catch (error) {
        console.error('Error:', error);
        errorMessage.textContent = 'אירעה שגיאה. נא לנסות שוב.';
        errorMessage.classList.add('show');
        submitBtn.disabled = false;
        submitBtn.innerHTML = originalContent;
    }
});

// Phone number formatting
document.getElementById('phone').addEventListener('input', function(e) {
    let value = e.target.value.replace(/[^\d]/g, '');
    
    if (value.length > 3 && value.length <= 7) {
        value = value.slice(0, 3) + '-' + value.slice(3);
    } else if (value.length > 7) {
        value = value.slice(0, 3) + '-' + value.slice(3, 10);
    }
    
    e.target.value = value;
});

// Save form data on input
['firstName', 'lastName', 'phone', 'email'].forEach(id => {
    document.getElementById(id).addEventListener('input', function() {
        formData[id] = this.value.trim();
    });
});

// Load config on page load
loadConfig();
