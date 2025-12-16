require('dotenv').config();

const config = {
    // Database Configuration
    database: {
        url: process.env.DATABASE_URL,
        poolSize: parseInt(process.env.DB_POOL_SIZE) || 20,
        idleTimeout: parseInt(process.env.DB_IDLE_TIMEOUT) || 30000,
        connectionTimeout: parseInt(process.env.DB_CONNECTION_TIMEOUT) || 2000,
        maxRetries: parseInt(process.env.DB_MAX_RETRIES) || 3,
        retryDelay: parseInt(process.env.DB_RETRY_DELAY) || 1000
    },

    // Google Sheets Configuration
    googleSheets: {
        spreadsheetId: process.env.GOOGLE_SHEET_ID || '1rJ7dfMnqkFJL5i0njsYVEPFCvlCiwKmofZHw2d9tq9k',
        serviceAccountPath: process.env.SERVICE_ACCOUNT_PATH || 'service-account.json',
        range: process.env.SHEET_RANGE || 'Sheet1!A2:J',
        batchSize: parseInt(process.env.SHEET_BATCH_SIZE) || 10,
        rateLimitDelay: parseInt(process.env.SHEET_RATE_LIMIT_DELAY) || 100
    },

    // ETL Configuration
    etl: {
        sourceType: process.env.SOURCE_TYPE || 'SHEET',
        csvPath: {
            students: process.env.STUDENTS_CSV_PATH || 'students.csv',
            netflix: process.env.NETFLIX_CSV_PATH || 'netflix.csv',
            titanic: process.env.TITANIC_CSV_PATH || 'titanic.csv'
        },
        jsonPath: {
            students: process.env.STUDENTS_JSON_PATH || 'students.json'
        },
        batchSize: parseInt(process.env.ETL_BATCH_SIZE) || 100,
        enableCaching: process.env.ENABLE_CACHING === 'true' || false,
        cacheExpiryMinutes: parseInt(process.env.CACHE_EXPIRY_MINUTES) || 30
    },

    // Logging Configuration
    logging: {
        level: process.env.LOG_LEVEL || 'info',
        file: process.env.LOG_FILE || 'logs/combined.log',
        errorFile: process.env.ERROR_LOG_FILE || 'logs/error.log'
    },

    // Environment
    env: process.env.NODE_ENV || 'development',
    isDevelopment: (process.env.NODE_ENV || 'development') === 'development',
    isProduction: process.env.NODE_ENV === 'production'
};

module.exports = config;
