#!/usr/bin/env node
// CLI for splitting CSV files using the same logic as the API
const path = require('path');
const dotenv = require('dotenv');
dotenv.config({ path: path.join(__dirname, '.env') });

const { splitCompanyContacts } = require('./src/services/contact/contact.service');

async function main() {
  const inputArg = process.argv[2];
  if (!inputArg) {
    console.error('Usage: node cli-split.js <input.csv>');
    process.exit(1);
  }
  // Set env var for input path
  process.env.CSV_INPUT_PATH = path.resolve(`./src/uploads/${inputArg}`);
  try {
    const result = await splitCompanyContacts();
    if (result && result.data) {
      console.log('\n=== CSV Split Summary ===');
      for (const [k, v] of Object.entries(result.data)) {
        console.log(`${k}: ${v}`);
      }
      console.log('========================\n');
    } else {
      console.log(result);
    }
  } catch (err) {
    console.error('Error:', err);
    process.exit(2);
  }
}

main();
