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

// Smooth transition between sections
function transitionTo(fromSection, toSection, beforeShow) {
    // Add fade out animation
    fromSection.classList.add('fade-out');
    
    // Wait for animation to complete
    setTimeout(() => {
        fromSection.style.display = 'none';
        fromSection.classList.remove('fade-out');
        
        // Execute callback before showing
        if (beforeShow) beforeShow();
        
        // Show new section with fade in
        toSection.style.display = 'block';
        toSection.classList.add('fade-in');
        
        // Remove animation class after completion
        setTimeout(() => {
            toSection.classList.remove('fade-in');
        }, 500);
    }, 400);
}

// Instructions checkbox handler
document.getElementById('readInstructions').addEventListener('change', function() {
    if (this.checked) {
        goToForm();
    }
});

function goToForm() {
    transitionTo(instructionsSection, formSection, () => {
        // Restore saved values
        document.getElementById('firstName').value = formData.firstName;
        document.getElementById('lastName').value = formData.lastName;
        document.getElementById('phone').value = formData.phone;
        document.getElementById('email').value = formData.email;
    });
}

// Back button handler
document.getElementById('backToInstructions').addEventListener('click', function(e) {
    e.preventDefault();
    
    // Save current values
    formData.firstName = document.getElementById('firstName').value.trim();
    formData.lastName = document.getElementById('lastName').value.trim();
    formData.phone = document.getElementById('phone').value.trim();
    formData.email = document.getElementById('email').value.trim();
    
    transitionTo(formSection, instructionsSection, () => {
        // Reset checkbox
        document.getElementById('readInstructions').checked = false;
    });
});

// Video Modal handlers
document.getElementById('openVideoBtn').addEventListener('click', function(e) {
    e.preventDefault();
    openVideoModal();
});

document.getElementById('closeVideoBtn').addEventListener('click', function(e) {
    e.preventDefault();
    closeVideoModal();
});

document.getElementById('videoWatched').addEventListener('change', function() {
    document.getElementById('continueFromVideo').disabled = !this.checked;
});

document.getElementById('continueFromVideo').addEventListener('click', function(e) {
    e.preventDefault();
    closeVideoModal();
    
    // Wait for modal to close, then transition to form
    setTimeout(() => {
        transitionTo(instructionsSection, formSection, () => {
            // Restore saved values
            document.getElementById('firstName').value = formData.firstName;
            document.getElementById('lastName').value = formData.lastName;
            document.getElementById('phone').value = formData.phone;
            document.getElementById('email').value = formData.email;
        });
    }, 300);
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
    
    // Reset video checkbox
    document.getElementById('videoWatched').checked = false;
    document.getElementById('continueFromVideo').disabled = true;
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

// Phone number formatting - handles multiple formats
document.getElementById('phone').addEventListener('input', function(e) {
    let value = e.target.value;
    
    // Remove everything except digits
    let digits = value.replace(/[^\d]/g, '');
    
    // Handle international format (972...)
    if (digits.startsWith('972')) {
        digits = '0' + digits.slice(3); // Convert 972 to 0
    }
    
    // Keep only 10 digits max (Israeli format)
    digits = digits.slice(0, 10);
    
    // Format as 05X-XXXXXXX
    if (digits.length > 3) {
        e.target.value = digits.slice(0, 3) + '-' + digits.slice(3);
    } else {
        e.target.value = digits;
    }
});

// Also handle paste event for phone
document.getElementById('phone').addEventListener('paste', function(e) {
    e.preventDefault();
    let pastedText = (e.clipboardData || window.clipboardData).getData('text');
    
    // Remove everything except digits
    let digits = pastedText.replace(/[^\d]/g, '');
    
    // Handle international format
    if (digits.startsWith('972')) {
        digits = '0' + digits.slice(3);
    }
    
    // Keep only 10 digits
    digits = digits.slice(0, 10);
    
    // Format and insert
    if (digits.length > 3) {
        this.value = digits.slice(0, 3) + '-' + digits.slice(3);
    } else {
        this.value = digits;
    }
    
    // Update formData
    formData.phone = this.value;
});

// Save form data on input
['firstName', 'lastName', 'phone', 'email'].forEach(id => {
    document.getElementById(id).addEventListener('input', function() {
        formData[id] = this.value.trim();
    });
});

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    // Hide form initially
    formSection.style.display = 'none';
    
    // Load config
    loadConfig();
    
    // Check for pre-filled parameters (reconnection link)
    const urlParams = new URLSearchParams(window.location.search);
    const isReconnect = urlParams.get('reconnect') === '1';
    
    if (isReconnect) {
        // Pre-fill form fields
        const phone = urlParams.get('phone') || '';
        const email = urlParams.get('email') || '';
        const name = urlParams.get('name') || '';
        
        // Split name into first and last
        const nameParts = name.split(' ');
        const firstName = nameParts[0] || '';
        const lastName = nameParts.slice(1).join(' ') || '';
        
        // Set form values
        document.getElementById('firstName').value = firstName;
        document.getElementById('lastName').value = lastName;
        document.getElementById('phone').value = formatPhoneDisplay(phone);
        document.getElementById('email').value = email;
        
        // Update formData
        formData.firstName = firstName;
        formData.lastName = lastName;
        formData.phone = phone;
        formData.email = email;
        
        // Skip to form directly
        instructionsSection.style.display = 'none';
        formSection.style.display = 'block';
    }
});

// Format phone for display
function formatPhoneDisplay(phone) {
    if (!phone) return '';
    let digits = phone.replace(/[^0-9]/g, '');
    
    // Convert 972 to 0
    if (digits.startsWith('972')) {
        digits = '0' + digits.slice(3);
    }
    
    // Format as 05X-XXXXXXX
    if (digits.length === 10 && digits.startsWith('0')) {
        return digits.slice(0, 3) + '-' + digits.slice(3);
    }
    
    return digits;
}
