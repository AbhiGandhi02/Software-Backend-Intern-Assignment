require('dotenv').config();
const fs = require('fs');
const csv = require('csv-parser');
const config = require('../config/config');
const logger = require('../utils/logger');
const { initializePool, executeTransaction, closePool } = require('../utils/db');
const { initializeSheetsClient, readSheetData, batchUpdateSheet } = require('../utils/sheets');
const { validateStudentRow } = require('../utils/validators');
const { readCache, writeCache } = require('../utils/cache');
const { generateQualityReport } = require('../utils/quality');

/**
 * Extract data from configured source (Sheet, CSV, or JSON)
 */
async function extractData() {
    logger.info(`Extracting data from source: ${config.etl.sourceType}...`);

    // Check cache first
    const cacheKey = `${config.etl.sourceType}_${config.googleSheets.spreadsheetId || config.etl.csvPath.students}`;
    const cachedData = readCache(cacheKey);

    if (cachedData) {
        logger.info('Using cached data');
        return cachedData;
    }

    let rows = [];

    if (config.etl.sourceType === 'SHEET') {
        rows = await readSheetData();
    } else if (config.etl.sourceType === 'JSON') {
        const rawData = fs.readFileSync(config.etl.jsonPath.students, 'utf8');
        const jsonData = JSON.parse(rawData);
        rows = jsonData.map(obj => [
            obj['Timestamp'],
            obj['Student Name'],
            obj['Email Address'],
            obj['Phone Number'],
            obj['Department'],
            obj['Course Name'],
            obj['Credits'],
            obj['Grade'],
            obj['Year Of Study'],
            "Pending Sync"
        ]);
    } else if (config.etl.sourceType === 'CSV') {
        rows = await new Promise((resolve, reject) => {
            const results = [];
            fs.createReadStream(config.etl.csvPath.students)
                .pipe(csv())
                .on('data', (data) => {
                    results.push([
                        data['Timestamp'],
                        data['Student Name'],
                        data['Email Address'],
                        data['Phone Number'],
                        data['Department'],
                        data['Course Name'],
                        data['Credits'],
                        data['Grade'],
                        data['Year Of Study'],
                        "Pending Sync"
                    ]);
                })
                .on('end', () => resolve(results))
                .on('error', (err) => reject(err));
        });
    }

    // Cache the extracted data
    writeCache(cacheKey, rows);

    return rows;
}

/**
 * Transform and validate row data
 */
function transformRow(row, rowIndex) {
    const validation = validateStudentRow(row);

    if (!validation.valid) {
        logger.warn(`Row ${rowIndex} validation failed: ${validation.errors.join(', ')}`);
        throw new Error(validation.errors.join('; '));
    }

    return validation.data;
}

/**
 * Load data into database using transaction
 */
async function loadData(transformedRows, client) {
    let successCount = 0;

    for (const rowData of transformedRows) {
        try {
            await client.query(
                `CALL register_student($1, $2, $3, $4)`,
                [rowData.firstName, rowData.lastName, rowData.email, rowData.course]
            );
            successCount++;
        } catch (err) {
            logger.error(`Failed to insert student ${rowData.email}:`, err.message);
            throw err; // Will trigger transaction rollback
        }
    }

    return successCount;
}

/**
 * Main ETL Pipeline
 */
async function runETL() {
    logger.info("=== Starting ETL Automation Pipeline ===");
    logger.info(`Environment: ${config.env}`);
    logger.info(`Source: ${config.etl.sourceType}`);

    try {
        // Initialize connections
        await initializePool();

        if (config.etl.sourceType === 'SHEET') {
            await initializeSheetsClient();
        }

        // EXTRACT
        const rows = await extractData();

        if (!rows || rows.length === 0) {
            logger.info('No data found to process.');
            return;
        }

        logger.info(`Extracted ${rows.length} rows`);

        // Prepare batch updates for Google Sheets
        const sheetUpdates = [];
        const transformedRows = [];
        let processedCount = 0;
        let errorCount = 0;

        // TRANSFORM
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const rowIndex = i + 2; // Sheet row number (accounting for header)
            const status = row[9];

            // Filter: Only process "Pending Sync" rows for Sheet source
            if (config.etl.sourceType === 'SHEET' && status !== 'Pending Sync') {
                continue;
            }

            logger.debug(`Processing Row ${rowIndex}: ${row[1]}`);

            try {
                const transformedData = transformRow(row, rowIndex);
                transformedData.rowIndex = rowIndex;
                transformedRows.push(transformedData);

                processedCount++;
            } catch (err) {
                errorCount++;
                logger.error(`Row ${rowIndex} transformation failed: ${err.message}`);

                // Add error update for Google Sheets
                if (config.etl.sourceType === 'SHEET') {
                    sheetUpdates.push({
                        range: `Sheet1!J${rowIndex}`,
                        values: [[`Error: ${err.message.substring(0, 100)}`]]
                    });
                }
            }
        }

        if (transformedRows.length === 0) {
            logger.info("No valid rows to process.");

            // Update error statuses if any
            if (sheetUpdates.length > 0 && config.etl.sourceType === 'SHEET') {
                await batchUpdateSheet(sheetUpdates);
            }

            return;
        }

        logger.info(`Validated ${transformedRows.length} rows, ${errorCount} errors`);

        // LOAD - Use transaction for data integrity
        const loadedCount = await executeTransaction(async (client) => {
            return await loadData(transformedRows, client);
        });

        logger.info(`Successfully loaded ${loadedCount} students to database`);

        // Update Google Sheets with success status (batch update)
        if (config.etl.sourceType === 'SHEET') {
            for (const rowData of transformedRows) {
                sheetUpdates.push({
                    range: `Sheet1!J${rowData.rowIndex}`,
                    values: [['Synced']]
                });
            }

            if (sheetUpdates.length > 0) {
                await batchUpdateSheet(sheetUpdates);
                logger.info(`Updated ${sheetUpdates.length} status cells in Google Sheets`);
            }
        }

        // DATA QUALITY CHECKS
        logger.info("Running data quality checks...");
        const qualityReport = await generateQualityReport('students', {
            uniqueColumn: 'email',
            requiredColumns: ['first_name', 'last_name', 'email'],
            validations: [
                { column: 'email', type: 'email' },
                { column: 'enrollment_year', type: 'numeric', min: 1, max: 5 }
            ]
        });

        if (!qualityReport.passed) {
            logger.warn('Data quality checks found issues:', qualityReport);
        } else {
            logger.info('All data quality checks passed');
        }

        // Summary
        logger.info("=== ETL Pipeline Completed Successfully ===");
        logger.info(`Total rows extracted: ${rows.length}`);
        logger.info(`Rows processed: ${processedCount}`);
        logger.info(`Rows loaded: ${loadedCount}`);
        logger.info(`Errors: ${errorCount}`);

    } catch (err) {
        logger.error("=== Pipeline Error ===", err);
        throw err;
    } finally {
        await closePool();
        logger.info("Database connections closed");
    }
}

// Run the pipeline
if (require.main === module) {
    runETL()
        .then(() => {
            logger.info("Pipeline execution finished");
            process.exit(0);
        })
        .catch((err) => {
            logger.error("Pipeline execution failed:", err);
            process.exit(1);
        });
}

module.exports = { runETL };