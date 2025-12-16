require('dotenv').config();
const fs = require('fs');
const csv = require('csv-parser');
const { Client } = require('pg');

const CSV_FILE = 'netflix.csv';
const db = new Client({ connectionString: process.env.DATABASE_URL });

async function loadNetflix() {
    console.log("Starting Netflix ETL...");
    
    try {
        await db.connect();
        const rows = [];

        await new Promise((resolve, reject) => {
            fs.createReadStream(CSV_FILE)
                .pipe(csv({ 
                    mapHeaders: ({ header }) => header.toLowerCase().trim() 
                }))
                .on('data', (row) => rows.push(row))
                .on('end', resolve)
                .on('error', reject);
        });

        console.log(`Extracted ${rows.length} titles. Loading to DB...`);

        let successCount = 0;
        for (const row of rows) {
            if (!row.show_id) continue;

            const releaseYear = parseInt(row.release_year) || null;

            await db.query(
                `INSERT INTO netflix (
                    show_id, type, title, director, cast_members, 
                    country, date_added, release_year, rating, 
                    duration, listed_in, description
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (show_id) DO NOTHING`,
                [
                    row.show_id,
                    row.type,
                    row.title,
                    row.director,
                    row.cast, 
                    row.country,
                    row.date_added,
                    releaseYear,
                    row.rating,
                    row.duration,
                    row.listed_in,
                    row.description
                ]
            );
            successCount++;
        }

        console.log(`Netflix Data Loaded Successfully! (${successCount} titles inserted)`);

    } catch (err) {
        console.error("Fatal Error:", err);
    } finally {
        await db.end();
    }
}

loadNetflix();