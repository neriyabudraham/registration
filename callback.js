// Parse URL parameters
function getUrlParams() {
    const params = new URLSearchParams(window.location.search);
    return {
        success: params.get('success'),
        error: params.get('error'),
        email: params.get('email'),
        state: params.get('state')
    };
}

// Show appropriate state
function showResult() {
    const params = getUrlParams();
    const loadingState = document.getElementById('loadingState');
    const successState = document.getElementById('successState');
    const errorState = document.getElementById('errorState');
    const userInfo = document.getElementById('userInfo');
    const errorMessage = document.getElementById('errorMessage');
    
    // Get saved registration data
    const savedData = JSON.parse(localStorage.getItem('registrationData') || '{}');
    
    // Hide loading
    loadingState.style.display = 'none';
    
    if (params.success === 'true' || params.email) {
        // Success state
        successState.style.display = 'block';
        
        // Show user info if available
        if (savedData.firstName || params.email) {
            let infoHtml = '';
            if (savedData.firstName) {
                infoHtml += `<p><strong>שם:</strong> ${savedData.firstName} ${savedData.lastName || ''}</p>`;
            }
            if (savedData.phone) {
                infoHtml += `<p><strong>טלפון:</strong> ${savedData.phone}</p>`;
            }
            if (params.email || savedData.email) {
                infoHtml += `<p><strong>אימייל:</strong> ${params.email || savedData.email}</p>`;
            }
            userInfo.innerHTML = infoHtml;
        } else {
            userInfo.style.display = 'none';
        }
        
        // Clear saved data
        localStorage.removeItem('registrationData');
        
    } else if (params.error) {
        // Error state
        errorState.style.display = 'block';
        
        // Show error message
        const errorMessages = {
            'access_denied': 'הגישה נדחתה. אנא אשר את ההרשאות הנדרשות.',
            'invalid_request': 'הבקשה לא תקינה. אנא נסה שוב.',
            'server_error': 'שגיאת שרת. אנא נסה שוב מאוחר יותר.',
            'temporarily_unavailable': 'השירות אינו זמין כרגע. אנא נסה שוב.',
            'consent_required': 'נדרש אישור הרשאות. אנא אשר את כל ההרשאות.',
        };
        
        errorMessage.textContent = errorMessages[params.error] || `שגיאה: ${params.error}`;
        
    } else {
        // No params - check if we have state data
        if (params.state) {
            try {
                const stateData = JSON.parse(atob(params.state));
                if (stateData.email) {
                    successState.style.display = 'block';
                    userInfo.innerHTML = `<p><strong>אימייל:</strong> ${stateData.email}</p>`;
                    return;
                }
            } catch (e) {
                console.error('Error parsing state:', e);
            }
        }
        
        // Default: show loading for a moment then redirect
        setTimeout(() => {
            if (!params.success && !params.error) {
                // If no result after timeout, show error
                loadingState.style.display = 'none';
                errorState.style.display = 'block';
                errorMessage.textContent = 'לא התקבלה תשובה מהשרת. אנא נסה שוב.';
            }
        }, 3000);
        
        // Keep loading visible for now
        loadingState.style.display = 'block';
    }
}

// Run on page load
document.addEventListener('DOMContentLoaded', showResult);
