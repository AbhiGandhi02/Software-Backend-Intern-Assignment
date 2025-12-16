const { Pool } = require('pg');
const config = require('../config/config');
const logger = require('./logger');

let pool = null;

/**
 * Sleep utility for retry logic
 */
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Initialize database connection pool with retry logic
 */
async function initializePool() {
    if (pool) {
        return pool;
    }

    pool = new Pool({
        connectionString: config.database.url,
        max: config.database.poolSize,
        idleTimeoutMillis: config.database.idleTimeout,
        connectionTimeoutMillis: config.database.connectionTimeout,
    });

    // Handle pool errors
    pool.on('error', (err) => {
        logger.error('Unexpected error on idle client', err);
    });

    // Test connection with retry logic
    let retries = 0;
    while (retries < config.database.maxRetries) {
        try {
            const client = await pool.connect();
            await client.query('SELECT NOW()');
            client.release();
            logger.info('Database connection pool initialized successfully');
            return pool;
        } catch (err) {
            retries++;
            logger.warn(`Database connection attempt ${retries} failed: ${err.message}`);

            if (retries >= config.database.maxRetries) {
                logger.error('Max retries reached. Could not connect to database.');
                throw new Error(`Database connection failed after ${retries} attempts: ${err.message}`);
            }

            await sleep(config.database.retryDelay * retries); // Exponential backoff
        }
    }
}

/**
 * Get database pool instance
 */
function getPool() {
    if (!pool) {
        throw new Error('Database pool not initialized. Call initializePool() first.');
    }
    return pool;
}

/**
 * Execute query with automatic retry on connection errors
 */
async function queryWithRetry(text, params, retries = 3) {
    let lastError;

    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const result = await pool.query(text, params);
            return result;
        } catch (err) {
            lastError = err;

            // Only retry on connection errors
            if (err.code === 'ECONNRESET' || err.code === 'ETIMEDOUT') {
                logger.warn(`Query attempt ${attempt} failed, retrying...`, err.message);
                await sleep(1000 * attempt);
            } else {
                // Don't retry on SQL errors
                throw err;
            }
        }
    }

    throw lastError;
}

/**
 * Execute transaction with rollback on error
 */
async function executeTransaction(callback) {
    const client = await pool.connect();

    try {
        await client.query('BEGIN');
        logger.debug('Transaction started');

        const result = await callback(client);

        await client.query('COMMIT');
        logger.debug('Transaction committed');

        return result;
    } catch (err) {
        await client.query('ROLLBACK');
        logger.error('Transaction rolled back due to error:', err);
        throw err;
    } finally {
        client.release();
    }
}

/**
 * Batch insert with transaction support
 */
async function batchInsert(tableName, columns, values, conflictAction = 'DO NOTHING') {
    if (!values || values.length === 0) {
        logger.warn('No values provided for batch insert');
        return { rowCount: 0 };
    }

    const columnCount = columns.length;
    const placeholders = [];
    const flatValues = [];

    values.forEach((row, rowIndex) => {
        const rowPlaceholders = [];
        for (let colIndex = 0; colIndex < columnCount; colIndex++) {
            const paramIndex = rowIndex * columnCount + colIndex + 1;
            rowPlaceholders.push(`$${paramIndex}`);
            flatValues.push(row[colIndex]);
        }
        placeholders.push(`(${rowPlaceholders.join(', ')})`);
    });

    const query = `
    INSERT INTO ${tableName} (${columns.join(', ')})
    VALUES ${placeholders.join(', ')}
    ON CONFLICT ${conflictAction}
  `;

    try {
        const result = await queryWithRetry(query, flatValues);
        logger.info(`Batch inserted ${result.rowCount} rows into ${tableName}`);
        return result;
    } catch (err) {
        logger.error(`Batch insert failed for ${tableName}:`, err);
        throw err;
    }
}

/**
 * Close database pool gracefully
 */
async function closePool() {
    if (pool) {
        await pool.end();
        logger.info('Database connection pool closed');
        pool = null;
    }
}

module.exports = {
    initializePool,
    getPool,
    queryWithRetry,
    executeTransaction,
    batchInsert,
    closePool
};
