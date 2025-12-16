const fs = require('fs');
const path = require('path');
const config = require('../config/config');
const logger = require('./logger');

const CACHE_DIR = path.join(__dirname, '..', '.cache');

/**
 * Ensure cache directory exists
 */
function ensureCacheDir() {
    if (!fs.existsSync(CACHE_DIR)) {
        fs.mkdirSync(CACHE_DIR, { recursive: true });
    }
}

/**
 * Generate cache key from identifier
 */
function getCacheKey(identifier) {
    return identifier.replace(/[^a-zA-Z0-9_-]/g, '_');
}

/**
 * Get cache file path
 */
function getCachePath(key) {
    return path.join(CACHE_DIR, `${getCacheKey(key)}.json`);
}

/**
 * Check if cache exists and is valid
 */
function isCacheValid(key) {
    if (!config.etl.enableCaching) {
        return false;
    }

    const cachePath = getCachePath(key);

    if (!fs.existsSync(cachePath)) {
        return false;
    }

    try {
        const stats = fs.statSync(cachePath);
        const cacheAge = Date.now() - stats.mtimeMs;
        const maxAge = config.etl.cacheExpiryMinutes * 60 * 1000;

        if (cacheAge > maxAge) {
            logger.debug(`Cache expired for key: ${key}`);
            return false;
        }

        logger.debug(`Cache valid for key: ${key}`);
        return true;
    } catch (err) {
        logger.warn(`Error checking cache validity for ${key}:`, err);
        return false;
    }
}

/**
 * Read from cache
 */
function readCache(key) {
    if (!config.etl.enableCaching) {
        return null;
    }

    if (!isCacheValid(key)) {
        return null;
    }

    const cachePath = getCachePath(key);

    try {
        const data = fs.readFileSync(cachePath, 'utf8');
        const parsed = JSON.parse(data);
        logger.info(`Cache hit for key: ${key}`);
        return parsed;
    } catch (err) {
        logger.warn(`Error reading cache for ${key}:`, err);
        return null;
    }
}

/**
 * Write to cache
 */
function writeCache(key, data) {
    if (!config.etl.enableCaching) {
        return;
    }

    ensureCacheDir();
    const cachePath = getCachePath(key);

    try {
        fs.writeFileSync(cachePath, JSON.stringify(data, null, 2), 'utf8');
        logger.info(`Cache written for key: ${key}`);
    } catch (err) {
        logger.error(`Error writing cache for ${key}:`, err);
    }
}

/**
 * Invalidate cache for a specific key
 */
function invalidateCache(key) {
    const cachePath = getCachePath(key);

    try {
        if (fs.existsSync(cachePath)) {
            fs.unlinkSync(cachePath);
            logger.info(`Cache invalidated for key: ${key}`);
        }
    } catch (err) {
        logger.error(`Error invalidating cache for ${key}:`, err);
    }
}

/**
 * Clear all cache
 */
function clearAllCache() {
    ensureCacheDir();

    try {
        const files = fs.readdirSync(CACHE_DIR);

        for (const file of files) {
            if (file.endsWith('.json')) {
                fs.unlinkSync(path.join(CACHE_DIR, file));
            }
        }

        logger.info('All cache cleared');
    } catch (err) {
        logger.error('Error clearing cache:', err);
    }
}

/**
 * Get cache statistics
 */
function getCacheStats() {
    ensureCacheDir();

    try {
        const files = fs.readdirSync(CACHE_DIR);
        const cacheFiles = files.filter(f => f.endsWith('.json'));

        const stats = {
            totalFiles: cacheFiles.length,
            files: []
        };

        for (const file of cacheFiles) {
            const filePath = path.join(CACHE_DIR, file);
            const fileStats = fs.statSync(filePath);

            stats.files.push({
                name: file,
                size: fileStats.size,
                modified: fileStats.mtime,
                age: Date.now() - fileStats.mtimeMs
            });
        }

        return stats;
    } catch (err) {
        logger.error('Error getting cache stats:', err);
        return null;
    }
}

module.exports = {
    isCacheValid,
    readCache,
    writeCache,
    invalidateCache,
    clearAllCache,
    getCacheStats
};
