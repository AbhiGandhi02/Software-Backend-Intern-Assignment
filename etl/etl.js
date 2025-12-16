require('dotenv').config();
const fs = require('fs');
const csv = require('csv-parser');
const { google } = require('googleapis');
const { Client } = require('pg');

// CONFIGURATION: CHANGE THIS TO 'SHEET', 'CSV', or 'JSON'
const SOURCE_TYPE = 'SHEET'; 

const GOOGLE_SHEET_ID = '1rJ7dfMnqkFJL5i0njsYVEPFCvlCiwKmofZHw2d9tq9k'; 
const CSV_FILE_PATH = 'students.csv';
const JSON_FILE_PATH = 'students.json';
const RANGE = 'Sheet1!A2:J'; 

// DB CONNECTION
const db = new Client({ connectionString: process.env.DATABASE_URL });

async function extractData(sheetsClient) {
    console.log(`Extracting data from source: ${SOURCE_TYPE}...`);
    
    // CASE 1: GOOGLE SHEETS (Smart Bridge Logic)
    if (SOURCE_TYPE === 'SHEET') {
        const res = await sheetsClient.spreadsheets.values.get({
            spreadsheetId: GOOGLE_SHEET_ID,
            range: RANGE,
        });
        return res.data.values;
    }

    // CASE 2: JSON FILE
    if (SOURCE_TYPE === 'JSON') {
        const rawData = fs.readFileSync(JSON_FILE_PATH);
        return JSON.parse(rawData).map(obj => [
            obj['Timestamp'], obj['Student Name'], obj['Email Address'], obj['Phone Number'],
            obj['Department'], obj['Course Name'], obj['Credits'], obj['Grade'], obj['Year Of Study'], "⏳ Pending Sync"
        ]);
    }

    // CASE 3: CSV FILE
    if (SOURCE_TYPE === 'CSV') {
        const results = [];
        return new Promise((resolve, reject) => {
            fs.createReadStream(CSV_FILE_PATH)
                .pipe(csv())
                .on('data', (data) => {
                    results.push([
                        data['Timestamp'], data['Student Name'], data['Email Address'], data['Phone Number'],
                        data['Department'], data['Course Name'], data['Credits'], data['Grade'], data['Year Of Study'], "⏳ Pending Sync"
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
        
        // Setup Google Auth (Needed for reading AND writing)
        const auth = new google.auth.GoogleAuth({
            keyFile: 'service-account.json',
            scopes: ['https://www.googleapis.com/auth/spreadsheets'],
        });
        const sheets = google.sheets({ version: 'v4', auth });

        // 1. EXTRACT
        const rows = await extractData(sheets);
        
        if (!rows || rows.length === 0) {
            console.log('⚠️ No data found.');
            return;
        }

        let processedCount = 0;

        // 2. TRANSFORM & LOAD
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const rowIndex = i + 2; 
            const status = row[9];  

            // FILTER: Only process "Pending Sync" rows (for Sheet mode)
            if (SOURCE_TYPE === 'SHEET' && status !== '⏳ Pending Sync') continue;

            console.log(`Processing Row ${rowIndex}: ${row[1]}...`);

            try {
                // Parse Data
                const nameParts = (row[1] || '').split(' ');
                const firstName = nameParts[0];
                const lastName = nameParts.slice(1).join(' ') || 'Unknown';
                const email = (row[2] || '').trim();
                const course = row[5] || 'General';

                // Skip invalid emails
                if (!email.includes('@')) {
                    throw new Error("Invalid Email");
                }

                // DB INSERT: Use Stored Procedure (Task 5)
                await db.query(`CALL register_student($1, $2, $3, $4)`, 
                    [firstName, lastName, email, course]);

                // WRITE BACK: Update Sheet Status 
                if (SOURCE_TYPE === 'SHEET') {
                    await sheets.spreadsheets.values.update({
                        spreadsheetId: GOOGLE_SHEET_ID,
                        range: `Sheet1!J${rowIndex}`,
                        valueInputOption: 'RAW',
                        resource: { values: [['✅ Synced']] }
                    });
                }
                
                processedCount++;
                console.log(`Success`);

            } catch (err) {
                console.error(`Failed:`, err.message);
                
                // WRITE BACK: Error Status
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
            console.log("No pending rows found.");
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