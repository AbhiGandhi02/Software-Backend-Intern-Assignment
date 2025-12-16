require('dotenv').config();
const { Client } = require('pg');

const client = new Client({
  connectionString: process.env.DATABASE_URL,
});

async function testConnection() {
  try {
    await client.connect();
    console.log("Success: Connected to PostgreSQL/NeonDB!");
    
    // Run a quick query to prove it
    const res = await client.query('SELECT NOW()');
    console.log("Database Time:", res.rows[0].now);
    
    await client.end();
  } catch (err) {
    console.error("Error connecting to DB:", err);
  }
}

testConnection();