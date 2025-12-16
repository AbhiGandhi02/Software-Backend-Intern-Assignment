const logger = require('./logger');
const { getPool } = require('./db');

/**
 * Verify row count after ETL
 */
async function verifyRowCount(tableName, expectedCount, tolerance = 0) {
    try {
        const pool = getPool();
        const result = await pool.query(`SELECT COUNT(*) FROM ${tableName}`);
        const actualCount = parseInt(result.rows[0].count);

        const diff = Math.abs(actualCount - expectedCount);

        if (diff > tolerance) {
            logger.error(`Row count mismatch for ${tableName}: expected ${expectedCount}, got ${actualCount}`);
            return {
                valid: false,
                expected: expectedCount,
                actual: actualCount,
                difference: diff
            };
        }

        logger.info(`Row count verification passed for ${tableName}: ${actualCount} rows`);
        return {
            valid: true,
            expected: expectedCount,
            actual: actualCount,
            difference: diff
        };
    } catch (err) {
        logger.error(`Error verifying row count for ${tableName}:`, err);
        throw err;
    }
}

/**
 * Check for duplicate records
 */
async function checkDuplicates(tableName, uniqueColumn) {
    try {
        const pool = getPool();
        const query = `
      SELECT ${uniqueColumn}, COUNT(*) as count
      FROM ${tableName}
      GROUP BY ${uniqueColumn}
      HAVING COUNT(*) > 1
    `;

        const result = await pool.query(query);

        if (result.rows.length > 0) {
            logger.warn(`Found ${result.rows.length} duplicate records in ${tableName}.${uniqueColumn}`);
            return {
                hasDuplicates: true,
                count: result.rows.length,
                duplicates: result.rows
            };
        }

        logger.info(`No duplicates found in ${tableName}.${uniqueColumn}`);
        return {
            hasDuplicates: false,
            count: 0
        };
    } catch (err) {
        logger.error(`Error checking duplicates for ${tableName}.${uniqueColumn}:`, err);
        throw err;
    }
}

/**
 * Check for null values in critical columns
 */
async function checkNullValues(tableName, columns) {
    try {
        const pool = getPool();
        const results = {};

        for (const column of columns) {
            const query = `SELECT COUNT(*) FROM ${tableName} WHERE ${column} IS NULL`;
            const result = await pool.query(query);
            const nullCount = parseInt(result.rows[0].count);

            results[column] = nullCount;

            if (nullCount > 0) {
                logger.warn(`Found ${nullCount} null values in ${tableName}.${column}`);
            }
        }

        logger.info(`Null value check completed for ${tableName}`);
        return results;
    } catch (err) {
        logger.error(`Error checking null values for ${tableName}:`, err);
        throw err;
    }
}

/**
 * Validate data types and ranges
 */
async function validateDataTypes(tableName, validations) {
    try {
        const pool = getPool();
        const issues = [];

        for (const validation of validations) {
            const { column, type, min, max } = validation;

            // Check numeric ranges
            if (type === 'numeric' && (min !== undefined || max !== undefined)) {
                let query = `SELECT COUNT(*) FROM ${tableName} WHERE `;
                const conditions = [];

                if (min !== undefined) {
                    conditions.push(`${column} < ${min}`);
                }
                if (max !== undefined) {
                    conditions.push(`${column} > ${max}`);
                }

                query += conditions.join(' OR ');

                const result = await pool.query(query);
                const outOfRangeCount = parseInt(result.rows[0].count);

                if (outOfRangeCount > 0) {
                    issues.push({
                        column,
                        issue: 'out_of_range',
                        count: outOfRangeCount,
                        message: `${outOfRangeCount} records have ${column} outside range [${min}, ${max}]`
                    });
                    logger.warn(`${outOfRangeCount} records in ${tableName}.${column} are out of range`);
                }
            }

            // Check email format
            if (type === 'email') {
                const query = `SELECT COUNT(*) FROM ${tableName} WHERE ${column} !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'`;
                const result = await pool.query(query);
                const invalidCount = parseInt(result.rows[0].count);

                if (invalidCount > 0) {
                    issues.push({
                        column,
                        issue: 'invalid_format',
                        count: invalidCount,
                        message: `${invalidCount} records have invalid email format in ${column}`
                    });
                    logger.warn(`${invalidCount} records in ${tableName}.${column} have invalid email format`);
                }
            }
        }

        logger.info(`Data type validation completed for ${tableName}: ${issues.length} issues found`);
        return {
            valid: issues.length === 0,
            issues
        };
    } catch (err) {
        logger.error(`Error validating data types for ${tableName}:`, err);
        throw err;
    }
}

/**
 * Generate data quality report
 */
async function generateQualityReport(tableName, config) {
    logger.info(`Generating data quality report for ${tableName}...`);

    const report = {
        tableName,
        timestamp: new Date().toISOString(),
        checks: {}
    };

    try {
        // Row count check
        if (config.expectedRowCount) {
            report.checks.rowCount = await verifyRowCount(
                tableName,
                config.expectedRowCount,
                config.rowCountTolerance || 0
            );
        }

        // Duplicate check
        if (config.uniqueColumn) {
            report.checks.duplicates = await checkDuplicates(tableName, config.uniqueColumn);
        }

        // Null value check
        if (config.requiredColumns) {
            report.checks.nullValues = await checkNullValues(tableName, config.requiredColumns);
        }

        // Data type validation
        if (config.validations) {
            report.checks.dataTypes = await validateDataTypes(tableName, config.validations);
        }

        // Overall status
        report.passed =
            (!report.checks.rowCount || report.checks.rowCount.valid) &&
            (!report.checks.duplicates || !report.checks.duplicates.hasDuplicates) &&
            (!report.checks.dataTypes || report.checks.dataTypes.valid);

        logger.info(`Data quality report generated for ${tableName}: ${report.passed ? 'PASSED' : 'FAILED'}`);
        return report;
    } catch (err) {
        logger.error(`Error generating quality report for ${tableName}:`, err);
        report.error = err.message;
        report.passed = false;
        return report;
    }
}

module.exports = {
    verifyRowCount,
    checkDuplicates,
    checkNullValues,
    validateDataTypes,
    generateQualityReport
};
