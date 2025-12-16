const winston = require('winston');
const path = require('path');
const fs = require('fs');
const config = require('../config/config');

// Ensure logs directory exists
const logsDir = path.dirname(config.logging.file);
if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
}

// Define log format
const logFormat = winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    winston.format.errors({ stack: true }),
    winston.format.splat(),
    winston.format.json()
);

// Define console format for development
const consoleFormat = winston.format.combine(
    winston.format.colorize(),
    winston.format.timestamp({ format: 'HH:mm:ss' }),
    winston.format.printf(({ timestamp, level, message, ...meta }) => {
        let msg = `${timestamp} [${level}]: ${message}`;
        if (Object.keys(meta).length > 0) {
            msg += ` ${JSON.stringify(meta)}`;
        }
        return msg;
    })
);

// Create logger instance
const logger = winston.createLogger({
    level: config.logging.level,
    format: logFormat,
    transports: [
        // Write all logs to combined.log
        new winston.transports.File({
            filename: config.logging.file,
            maxsize: 5242880, // 5MB
            maxFiles: 5
        }),
        // Write errors to error.log
        new winston.transports.File({
            filename: config.logging.errorFile,
            level: 'error',
            maxsize: 5242880, // 5MB
            maxFiles: 5
        })
    ]
});

// Add console logging in development
if (config.isDevelopment) {
    logger.add(new winston.transports.Console({
        format: consoleFormat
    }));
}

module.exports = logger;
