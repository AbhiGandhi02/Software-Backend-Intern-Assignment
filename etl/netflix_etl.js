require('dotenv').config();
const fs = require('fs');
const csv = require('csv-parser');
const config = require('../config/config');
const logger = require('../utils/logger');
const { initializePool, batchInsert, executeTransaction, closePool } = require('../utils/db');
const { validateNetflixRow } = require('../utils/validators');
const { generateQualityReport } = require('../utils/quality');

/**
 * Extract Netflix data from CSV
 */
async function extractNetflixData() {
    logger.info("Extracting Netflix data from CSV...");
    const rows = [];

    await new Promise((resolve, reject) => {
        fs.createReadStream(config.etl.csvPath.netflix)
            .pipe(csv({
                mapHeaders: ({ header }) => header.toLowerCase().trim()
            }))
            .on('data', (row) => rows.push(row))
            .on('end', resolve)
            .on('error', reject);
    });

    logger.info(`Extracted ${rows.length} Netflix titles`);
    return rows;
}

/**
 * Transform and validate Netflix data
 */
function transformNetflixData(rows) {
    logger.info("Transforming and validating Netflix data...");
    const validRows = [];
    let errorCount = 0;

    for (const row of rows) {
        const validation = validateNetflixRow(row);

        if (!validation.valid || !validation.data.show_id) {
            errorCount++;
            logger.warn(`Invalid Netflix row: ${validation.error || 'Missing show_id'}`);
            continue;
        }

        validRows.push(validation.data);
    }

    logger.info(`Validated ${validRows.length} rows, ${errorCount} errors`);
    return validRows;
}

/**
 * Load Netflix data using batch insert
 */
async function loadNetflixData(validRows) {
    logger.info("Loading Netflix data to database...");

    const columns = [
        'show_id', 'type', 'title', 'director', 'cast_members',
        'country', 'date_added', 'release_year', 'rating',
        'duration', 'listed_in', 'description'
    ];

    // Prepare values array
    const values = validRows.map(row => [
        row.show_id,
        row.type,
        row.title,
        row.director,
        row.cast_members,
        row.country,
        row.date_added,
        row.release_year,
        row.rating,
        row.duration,
        row.listed_in,
        row.description
    ]);

    // Use batch insert with transaction
    let insertedCount = 0;

    await executeTransaction(async (client) => {
        // Process in smaller batches to avoid parameter limit
        const batchSize = config.etl.batchSize;

        for (let i = 0; i < values.length; i += batchSize) {
            const batch = values.slice(i, i + batchSize);

            // Build batch insert query
            const placeholders = batch.map((_, batchIdx) => {
                const offset = batchIdx * columns.length;
                return `(${columns.map((_, colIdx) => `$${offset + colIdx + 1}`).join(', ')})`;
            }).join(', ');

            const query = `
        INSERT INTO netflix (${columns.join(', ')})
        VALUES ${placeholders}
        ON CONFLICT (show_id) DO NOTHING
      `;

            const result = await client.query(query, batch.flat());
            insertedCount += result.rowCount;

            logger.debug(`Batch ${Math.floor(i / batchSize) + 1}: Inserted ${result.rowCount} rows`);
        }
    });

    logger.info(`Netflix data loaded successfully! (${insertedCount} titles inserted)`);
    return insertedCount;
}

/**
 * Main Netflix ETL Pipeline
 */
async function loadNetflix() {
    logger.info("=== Starting Netflix ETL Pipeline ===");

    try {
        // Initialize database pool
        await initializePool();

        // EXTRACT
        const rows = await extractNetflixData();

        // TRANSFORM
        const validRows = transformNetflixData(rows);

        if (validRows.length === 0) {
            logger.warn("No valid Netflix data to load");
            return;
        }

        // LOAD
        const insertedCount = await loadNetflixData(validRows);

        // DATA QUALITY CHECKS
        logger.info("Running data quality checks...");
        const qualityReport = await generateQualityReport('netflix', {
            expectedRowCount: validRows.length,
            rowCountTolerance: 10,
            uniqueColumn: 'show_id',
            requiredColumns: ['show_id', 'title'],
            validations: [
                { column: 'release_year', type: 'numeric', min: 1900, max: 2030 }
            ]
        });

        if (!qualityReport.passed) {
            logger.warn('Data quality checks found issues:', qualityReport);
        } else {
            logger.info('All data quality checks passed');
        }

        // Summary
        logger.info("=== Netflix ETL Completed Successfully ===");
        logger.info(`Total rows extracted: ${rows.length}`);
        logger.info(`Valid rows: ${validRows.length}`);
        logger.info(`Rows inserted: ${insertedCount}`);

    } catch (err) {
        logger.error("=== Netflix ETL Error ===", err);
        throw err;
    } finally {
        await closePool();
        logger.info("Database connections closed");
    }
}

// Run the pipeline
if (require.main === module) {
    loadNetflix()
        .then(() => {
            logger.info("Netflix ETL execution finished");
            process.exit(0);
        })
        .catch((err) => {
            logger.error("Netflix ETL execution failed:", err);
            process.exit(1);
        });
}

module.exports = { loadNetflix };