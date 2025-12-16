require('dotenv').config();
const fs = require('fs');
const csv = require('csv-parser');
const { google } = require('googleapis');
const { Client } = require('pg');

// CONFIGURATION
const SOURCE_TYPE = 'SHEET'; 
const GOOGLE_SHEET_ID = '1rJ7dfMnqkFJL5i0njsYVEPFCvlCiwKmofZHw2d9tq9k'; 
const CSV_FILE_PATH = 'students.csv';
const JSON_FILE_PATH = 'students.json';
const RANGE = 'Sheet1!A2:J'; 

const db = new Client({ connectionString: process.env.DATABASE_URL });

async function extractData(sheetsClient) {
    console.log(`Extracting data from source: ${SOURCE_TYPE}...`);
    
    if (SOURCE_TYPE === 'SHEET') {
        const res = await sheetsClient.spreadsheets.values.get({
            spreadsheetId: GOOGLE_SHEET_ID,
            range: RANGE,
        });
        return res.data.values;
    }

    // JSON Handler
    if (SOURCE_TYPE === 'JSON') {
        const rawData = fs.readFileSync(JSON_FILE_PATH);
        return JSON.parse(rawData).map(obj => [
            obj['Timestamp'], obj['Student Name'], obj['Email Address'], obj['Phone Number'],
            obj['Department'], obj['Course Name'], obj['Credits'], obj['Grade'], obj['Year Of Study'], "Pending Sync"
        ]);
    }

    // CSV Handler
    if (SOURCE_TYPE === 'CSV') {
        const results = [];
        return new Promise((resolve, reject) => {
            fs.createReadStream(CSV_FILE_PATH)
                .pipe(csv())
                .on('data', (data) => {
                    results.push([
                        data['Timestamp'], data['Student Name'], data['Email Address'], data['Phone Number'],
                        data['Department'], data['Course Name'], data['Credits'], data['Grade'], data['Year Of Study'], "Pending Sync"
                    ]);
                })
                .on('end', () => resolve(results))
                .on('error', (err) => reject(err));
        });
    }
}

async function runETL() {
    console.log("Starting Automation Pipeline...");
    
    try {
        await db.connect();
        
        const auth = new google.auth.GoogleAuth({
            keyFile: 'service-account.json',
            scopes: ['https://www.googleapis.com/auth/spreadsheets'],
        });
        const sheets = google.sheets({ version: 'v4', auth });

        // 1. EXTRACT
        const rows = await extractData(sheets);
        
        if (!rows || rows.length === 0) {
            console.log('No data found.');
            return;
        }

        let processedCount = 0;

        // 2. TRANSFORM & LOAD
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const rowIndex = i + 2; 
            const status = row[9];  

            // FILTER: Strict check for plain text "Pending Sync"
            if (SOURCE_TYPE === 'SHEET' && status !== 'Pending Sync') continue;

            console.log(`Processing Row ${rowIndex}: ${row[1]}...`);

            try {
                const nameParts = (row[1] || '').split(' ');
                const firstName = nameParts[0];
                const lastName = nameParts.slice(1).join(' ') || 'Unknown';
                const email = (row[2] || '').trim();
                const course = row[5] || 'General';

                if (!email.includes('@')) throw new Error("Invalid Email");

                // DB INSERT
                await db.query(`CALL register_student($1, $2, $3, $4)`, 
                    [firstName, lastName, email, course]);

                // WRITE BACK: "Synced" (Plain text)
                if (SOURCE_TYPE === 'SHEET') {
                    await sheets.spreadsheets.values.update({
                        spreadsheetId: GOOGLE_SHEET_ID,
                        range: `Sheet1!J${rowIndex}`,
                        valueInputOption: 'RAW',
                        resource: { values: [['Synced']] }
                    });
                }
                
                processedCount++;
                console.log(`   Success`);

            } catch (err) {
                console.error(`   Failed:`, err.message);
                
                // WRITE BACK: Error
                if (SOURCE_TYPE === 'SHEET') {
                    await sheets.spreadsheets.values.update({
                        spreadsheetId: GOOGLE_SHEET_ID,
                        range: `Sheet1!J${rowIndex}`,
                        valueInputOption: 'RAW',
                        resource: { values: [[`Error: ${err.message}`]] }
                    });
                }
            }
        }

        if (processedCount === 0) {
            console.log("No 'Pending Sync' rows found.");
        } else {
            console.log(`Processed ${processedCount} rows!`);
        }

    } catch (err) {
        console.error("Pipeline Error:", err);
    } finally {
        await db.end();
    }
}

runETL();