require('dotenv').config();
const fs = require('fs');
const csv = require('csv-parser');
const { Client } = require('pg');

const CSV_FILE = 'titanic.csv';
const db = new Client({ connectionString: process.env.DATABASE_URL });

async function loadTitanic() {
    console.log("Starting Titanic ETL...");
    
    try {
        await db.connect();
        const rows = [];

        // 1. Extract (Read CSV) & Force Lowercase Headers
        await new Promise((resolve, reject) => {
            fs.createReadStream(CSV_FILE)
                .pipe(csv({ mapHeaders: ({ header }) => header.toLowerCase().trim() })) 
                .on('data', (row) => rows.push(row))
                .on('end', resolve)
                .on('error', reject);
        });

        console.log(`Extracted ${rows.length} rows.`);
        
        if (rows.length > 0) {
            console.log("Sample Row Data:", rows[0]);
        }

        let successCount = 0;
        
        // 2. Load (Insert into DB)
        for (const row of rows) {
            const passengerId = parseInt(row.passengerid);

            if (isNaN(passengerId)) {
                continue; 
            }

            const age = row.age ? parseFloat(row.age) : null;
            const fare = row.fare ? parseFloat(row.fare) : 0.0;
            const sibsp = row.sibsp ? parseInt(row.sibsp) : 0;
            const parch = row.parch ? parseInt(row.parch) : 0;
            const pclass = parseInt(row.pclass) || 3;
            const survived = parseInt(row.survived) || 0;

            await db.query(
                `INSERT INTO titanic (passenger_id, survived, pclass, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                 ON CONFLICT (passenger_id) DO NOTHING`,
                [
                    passengerId, survived, pclass, row.name, row.sex, 
                    age, sibsp, parch, row.ticket, fare, row.cabin, row.embarked
                ]
            );
            successCount++;
        }

        console.log(`Titanic Data Loaded Successfully! (${successCount} passengers inserted)`);

    } catch (err) {
        console.error("Fatal Error:", err);
    } finally {
        await db.end();
    }
}

loadTitanic();