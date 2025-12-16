const { google } = require('googleapis');
const config = require('../config/config');
const logger = require('./logger');

let sheetsClient = null;

/**
 * Sleep utility for rate limiting
 */
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Initialize Google Sheets client
 */
async function initializeSheetsClient() {
    if (sheetsClient) {
        return sheetsClient;
    }

    try {
        const auth = new google.auth.GoogleAuth({
            keyFile: config.googleSheets.serviceAccountPath,
            scopes: ['https://www.googleapis.com/auth/spreadsheets'],
        });

        sheetsClient = google.sheets({ version: 'v4', auth });
        logger.info('Google Sheets client initialized successfully');
        return sheetsClient;
    } catch (err) {
        logger.error('Failed to initialize Google Sheets client:', err);
        throw err;
    }
}

/**
 * Get Google Sheets client instance
 */
function getSheetsClient() {
    if (!sheetsClient) {
        throw new Error('Google Sheets client not initialized. Call initializeSheetsClient() first.');
    }
    return sheetsClient;
}

/**
 * Read data from Google Sheets
 */
async function readSheetData(spreadsheetId = null, range = null) {
    const sheetId = spreadsheetId || config.googleSheets.spreadsheetId;
    const sheetRange = range || config.googleSheets.range;

    try {
        const sheets = getSheetsClient();
        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: sheetId,
            range: sheetRange,
        });

        const rows = response.data.values || [];
        logger.info(`Read ${rows.length} rows from Google Sheets`);
        return rows;
    } catch (err) {
        logger.error('Failed to read from Google Sheets:', err);
        throw err;
    }
}

/**
 * Batch update Google Sheets with rate limiting
 */
async function batchUpdateSheet(updates, spreadsheetId = null) {
    if (!updates || updates.length === 0) {
        logger.warn('No updates to write to Google Sheets');
        return;
    }

    const sheetId = spreadsheetId || config.googleSheets.spreadsheetId;
    const sheets = getSheetsClient();
    const batchSize = config.googleSheets.batchSize;
    const delay = config.googleSheets.rateLimitDelay;

    logger.info(`Preparing to batch update ${updates.length} cells in Google Sheets`);

    // Split updates into batches to respect rate limits
    for (let i = 0; i < updates.length; i += batchSize) {
        const batch = updates.slice(i, i + batchSize);

        try {
            await sheets.spreadsheets.values.batchUpdate({
                spreadsheetId: sheetId,
                resource: {
                    data: batch,
                    valueInputOption: 'RAW'
                }
            });

            logger.debug(`Batch updated ${batch.length} cells (batch ${Math.floor(i / batchSize) + 1})`);

            // Rate limiting delay between batches
            if (i + batchSize < updates.length) {
                await sleep(delay);
            }
        } catch (err) {
            logger.error(`Failed to update batch starting at index ${i}:`, err);
            throw err;
        }
    }

    logger.info(`Successfully batch updated ${updates.length} cells to Google Sheets`);
}

/**
 * Update single cell (convenience wrapper)
 */
async function updateCell(range, value, spreadsheetId = null) {
    const sheetId = spreadsheetId || config.googleSheets.spreadsheetId;
    const sheets = getSheetsClient();

    try {
        await sheets.spreadsheets.values.update({
            spreadsheetId: sheetId,
            range: range,
            valueInputOption: 'RAW',
            resource: { values: [[value]] }
        });

        logger.debug(`Updated cell ${range} with value: ${value}`);
    } catch (err) {
        logger.error(`Failed to update cell ${range}:`, err);
        throw err;
    }
}

/**
 * Get sheet metadata (for caching - ETag support)
 */
async function getSheetMetadata(spreadsheetId = null) {
    const sheetId = spreadsheetId || config.googleSheets.spreadsheetId;
    const sheets = getSheetsClient();

    try {
        const response = await sheets.spreadsheets.get({
            spreadsheetId: sheetId,
            fields: 'spreadsheetId,properties.title,sheets.properties'
        });

        return {
            id: response.data.spreadsheetId,
            title: response.data.properties.title,
            sheets: response.data.sheets.map(s => ({
                sheetId: s.properties.sheetId,
                title: s.properties.title,
                index: s.properties.index
            }))
        };
    } catch (err) {
        logger.error('Failed to get sheet metadata:', err);
        throw err;
    }
}

module.exports = {
    initializeSheetsClient,
    getSheetsClient,
    readSheetData,
    batchUpdateSheet,
    updateCell,
    getSheetMetadata
};
