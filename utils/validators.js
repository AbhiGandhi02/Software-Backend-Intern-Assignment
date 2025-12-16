const validator = require('validator');
const logger = require('./logger');

/**
 * Sanitize string input - remove special characters and limit length
 */
function sanitizeString(input, maxLength = 255) {
    if (!input || typeof input !== 'string') {
        return '';
    }

    return input
        .trim()
        .replace(/[<>'"]/g, '') // Remove potential XSS characters
        .substring(0, maxLength);
}

/**
 * Sanitize name fields
 */
function sanitizeName(name, maxLength = 50) {
    if (!name || typeof name !== 'string') {
        return 'Unknown';
    }

    return name
        .trim()
        .replace(/[^\w\s-]/g, '') // Only allow word characters, spaces, and hyphens
        .replace(/\s+/g, ' ') // Collapse multiple spaces
        .substring(0, maxLength);
}

/**
 * Validate and sanitize email
 */
function validateEmail(email) {
    if (!email || typeof email !== 'string') {
        return { valid: false, error: 'Email is required' };
    }

    const sanitized = email.trim().toLowerCase();

    if (!validator.isEmail(sanitized)) {
        return { valid: false, error: 'Invalid email format' };
    }

    if (sanitized.length > 100) {
        return { valid: false, error: 'Email too long (max 100 characters)' };
    }

    return { valid: true, email: sanitized };
}

/**
 * Validate and sanitize phone number
 */
function validatePhone(phone) {
    if (!phone) {
        return { valid: true, phone: null }; // Phone is optional
    }

    const sanitized = phone.replace(/[^\d+\-() ]/g, '').substring(0, 20);

    return { valid: true, phone: sanitized };
}

/**
 * Validate numeric input
 */
function validateNumber(value, min = null, max = null) {
    const num = parseFloat(value);

    if (isNaN(num)) {
        return { valid: false, error: 'Invalid number' };
    }

    if (min !== null && num < min) {
        return { valid: false, error: `Number must be at least ${min}` };
    }

    if (max !== null && num > max) {
        return { valid: false, error: `Number must be at most ${max}` };
    }

    return { valid: true, value: num };
}

/**
 * Validate integer input
 */
function validateInteger(value, min = null, max = null) {
    const num = parseInt(value);

    if (isNaN(num)) {
        return { valid: false, error: 'Invalid integer' };
    }

    if (min !== null && num < min) {
        return { valid: false, error: `Integer must be at least ${min}` };
    }

    if (max !== null && num > max) {
        return { valid: false, error: `Integer must be at most ${max}` };
    }

    return { valid: true, value: num };
}

/**
 * Parse and validate full name into first and last name
 */
function parseFullName(fullName) {
    if (!fullName || typeof fullName !== 'string') {
        return {
            valid: false,
            error: 'Name is required'
        };
    }

    const sanitized = fullName.trim().replace(/\s+/g, ' ');
    const parts = sanitized.split(' ');

    if (parts.length === 0 || parts[0] === '') {
        return {
            valid: false,
            error: 'Name cannot be empty'
        };
    }

    const firstName = sanitizeName(parts[0]);
    const lastName = sanitizeName(parts.slice(1).join(' ') || 'Unknown');

    if (!firstName || firstName === 'Unknown') {
        return {
            valid: false,
            error: 'First name is required'
        };
    }

    return {
        valid: true,
        firstName,
        lastName
    };
}

/**
 * Validate student data row from Google Sheets/CSV
 */
function validateStudentRow(row) {
    const errors = [];
    const sanitized = {};

    // Validate name (index 1)
    const nameResult = parseFullName(row[1]);
    if (!nameResult.valid) {
        errors.push(`Name: ${nameResult.error}`);
    } else {
        sanitized.firstName = nameResult.firstName;
        sanitized.lastName = nameResult.lastName;
    }

    // Validate email (index 2)
    const emailResult = validateEmail(row[2]);
    if (!emailResult.valid) {
        errors.push(`Email: ${emailResult.error}`);
    } else {
        sanitized.email = emailResult.email;
    }

    // Validate phone (index 3) - optional
    const phoneResult = validatePhone(row[3]);
    sanitized.phone = phoneResult.phone;

    // Validate department (index 4)
    sanitized.department = sanitizeString(row[4] || 'General', 100);

    // Validate course (index 5)
    sanitized.course = sanitizeString(row[5] || 'General', 100);

    // Validate credits (index 6)
    const creditsResult = validateInteger(row[6], 1, 10);
    if (row[6] && !creditsResult.valid) {
        errors.push(`Credits: ${creditsResult.error}`);
    }
    sanitized.credits = creditsResult.valid ? creditsResult.value : null;

    // Validate grade (index 7)
    const grade = row[7] ? sanitizeString(row[7], 5).toUpperCase() : null;
    if (grade && !['A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D', 'F'].includes(grade)) {
        errors.push(`Grade: Invalid grade value '${grade}'`);
    }
    sanitized.grade = grade;

    // Validate year (index 8)
    const yearResult = validateInteger(row[8], 1, 5);
    if (row[8] && !yearResult.valid) {
        errors.push(`Year: ${yearResult.error}`);
    }
    sanitized.year = yearResult.valid ? yearResult.value : 1;

    return {
        valid: errors.length === 0,
        errors,
        data: sanitized
    };
}

/**
 * Validate Netflix row data
 */
function validateNetflixRow(row) {
    if (!row.show_id) {
        return { valid: false, error: 'show_id is required' };
    }

    const yearResult = validateInteger(row.release_year, 1900, 2030);

    return {
        valid: true,
        data: {
            show_id: sanitizeString(row.show_id, 10),
            type: sanitizeString(row.type, 20),
            title: sanitizeString(row.title, 255),
            director: sanitizeString(row.director, 500),
            cast_members: sanitizeString(row.cast, 1000),
            country: sanitizeString(row.country, 255),
            date_added: sanitizeString(row.date_added, 50),
            release_year: yearResult.valid ? yearResult.value : null,
            rating: sanitizeString(row.rating, 20),
            duration: sanitizeString(row.duration, 20),
            listed_in: sanitizeString(row.listed_in, 500),
            description: sanitizeString(row.description, 1000)
        }
    };
}

/**
 * Validate Titanic row data
 */
function validateTitanicRow(row) {
    const passengerIdResult = validateInteger(row.passengerid);

    if (!passengerIdResult.valid) {
        return { valid: false, error: 'Invalid passenger_id' };
    }

    const ageResult = validateNumber(row.age, 0, 150);
    const fareResult = validateNumber(row.fare, 0);
    const sibspResult = validateInteger(row.sibsp, 0, 10);
    const parchResult = validateInteger(row.parch, 0, 10);

    return {
        valid: true,
        data: {
            passenger_id: passengerIdResult.value,
            survived: validateInteger(row.survived, 0, 1).value || 0,
            pclass: validateInteger(row.pclass, 1, 3).value || 3,
            name: sanitizeString(row.name, 255),
            sex: sanitizeString(row.sex, 10),
            age: ageResult.valid ? ageResult.value : null,
            sibsp: sibspResult.valid ? sibspResult.value : 0,
            parch: parchResult.valid ? parchResult.value : 0,
            ticket: sanitizeString(row.ticket, 50),
            fare: fareResult.valid ? fareResult.value : 0.0,
            cabin: sanitizeString(row.cabin, 50),
            embarked: sanitizeString(row.embarked, 5)
        }
    };
}

module.exports = {
    sanitizeString,
    sanitizeName,
    validateEmail,
    validatePhone,
    validateNumber,
    validateInteger,
    parseFullName,
    validateStudentRow,
    validateNetflixRow,
    validateTitanicRow
};
