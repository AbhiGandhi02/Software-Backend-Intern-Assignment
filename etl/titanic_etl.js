require('dotenv').config();
const fs = require('fs');
const csv = require('csv-parser');
const config = require('../config/config');
const logger = require('../utils/logger');
const { initializePool, executeTransaction, closePool } = require('../utils/db');
const { validateTitanicRow } = require('../utils/validators');
const { generateQualityReport } = require('../utils/quality');

/**
 * Extract Titanic data from CSV
 */
async function extractTitanicData() {
    logger.info("Extracting Titanic data from CSV...");
    const rows = [];

    await new Promise((resolve, reject) => {
        fs.createReadStream(config.etl.csvPath.titanic)
            .pipe(csv({
                mapHeaders: ({ header }) => header.toLowerCase().trim()
            }))
            .on('data', (row) => rows.push(row))
            .on('end', resolve)
            .on('error', reject);
    });

    logger.info(`Extracted ${rows.length} Titanic passenger records`);
    return rows;
}

/**
 * Transform and validate Titanic data
 */
function transformTitanicData(rows) {
    logger.info("Transforming and validating Titanic data...");
    const validRows = [];
    let errorCount = 0;

    for (const row of rows) {
        const validation = validateTitanicRow(row);

        if (!validation.valid) {
            errorCount++;
            logger.warn(`Invalid Titanic row: ${validation.error}`);
            continue;
        }

        validRows.push(validation.data);
    }

    logger.info(`Validated ${validRows.length} rows, ${errorCount} errors`);
    return validRows;
}

/**
 * Load Titanic data using batch insert with transaction
 */
async function loadTitanicData(validRows) {
    logger.info("Loading Titanic data to database...");

    const columns = [
        'passenger_id', 'survived', 'pclass', 'name', 'sex', 'age',
        'sibsp', 'parch', 'ticket', 'fare', 'cabin', 'embarked'
    ];

    // Prepare values array
    const values = validRows.map(row => [
        row.passenger_id,
        row.survived,
        row.pclass,
        row.name,
        row.sex,
        row.age,
        row.sibsp,
        row.parch,
        row.ticket,
        row.fare,
        row.cabin,
        row.embarked
    ]);

    let insertedCount = 0;

    await executeTransaction(async (client) => {
        // Process in batches
        const batchSize = config.etl.batchSize;

        for (let i = 0; i < values.length; i += batchSize) {
            const batch = values.slice(i, i + batchSize);

            // Build batch insert query
            const placeholders = batch.map((_, batchIdx) => {
                const offset = batchIdx * columns.length;
                return `(${columns.map((_, colIdx) => `$${offset + colIdx + 1}`).join(', ')})`;
            }).join(', ');

            const query = `
        INSERT INTO titanic (${columns.join(', ')})
        VALUES ${placeholders}
        ON CONFLICT (passenger_id) DO NOTHING
      `;

            const result = await client.query(query, batch.flat());
            insertedCount += result.rowCount;

            logger.debug(`Batch ${Math.floor(i / batchSize) + 1}: Inserted ${result.rowCount} rows`);
        }
    });

    logger.info(`Titanic data loaded successfully! (${insertedCount} passengers inserted)`);
    return insertedCount;
}

/**
 * Main Titanic ETL Pipeline
 */
async function loadTitanic() {
    logger.info("=== Starting Titanic ETL Pipeline ===");

    try {
        // Initialize database pool
        await initializePool();

        // EXTRACT
        const rows = await extractTitanicData();

        // TRANSFORM
        const validRows = transformTitanicData(rows);

        if (validRows.length === 0) {
            logger.warn("No valid Titanic data to load");
            return;
        }

        // LOAD
        const insertedCount = await loadTitanicData(validRows);

        // DATA QUALITY CHECKS
        logger.info("Running data quality checks...");
        const qualityReport = await generateQualityReport('titanic', {
            expectedRowCount: validRows.length,
            rowCountTolerance: 5,
            uniqueColumn: 'passenger_id',
            requiredColumns: ['passenger_id', 'name'],
            validations: [
                { column: 'survived', type: 'numeric', min: 0, max: 1 },
                { column: 'pclass', type: 'numeric', min: 1, max: 3 },
                { column: 'age', type: 'numeric', min: 0, max: 150 },
                { column: 'fare', type: 'numeric', min: 0 }
            ]
        });

        if (!qualityReport.passed) {
            logger.warn('Data quality checks found issues:', qualityReport);
        } else {
            logger.info('All data quality checks passed');
        }

        // Summary
        logger.info("=== Titanic ETL Completed Successfully ===");
        logger.info(`Total rows extracted: ${rows.length}`);
        logger.info(`Valid rows: ${validRows.length}`);
        logger.info(`Rows inserted: ${insertedCount}`);

    } catch (err) {
        logger.error("=== Titanic ETL Error ===", err);
        throw err;
    } finally {
        await closePool();
        logger.info("Database connections closed");
    }
}

// Run the pipeline
if (require.main === module) {
    loadTitanic()
        .then(() => {
            logger.info("Titanic ETL execution finished");
            process.exit(0);
        })
        .catch((err) => {
            logger.error("Titanic ETL execution failed:", err);
            process.exit(1);
        });
}

module.exports = { loadTitanic };