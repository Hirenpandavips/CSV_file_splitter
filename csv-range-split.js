#!/usr/bin/env node
// CLI for splitting a CSV by row range and batch count
const path = require('path');
const dotenv = require('dotenv');
dotenv.config({ path: path.join(__dirname, '.env') });

const { splitCompanyContactsInRange } = require('./src/services/contact/contact.service');

async function main() {
  let inputArg = process.argv[2];
  const startArg = process.argv[3];
  const endArg = process.argv[4];
  const batchCountArg = process.argv[5];
  if (!inputArg || !startArg || !endArg) {
    console.error('Usage: node csv-range-split.js <input.csv> <start> <end> [batchCount]');
    process.exit(1);
  }
  const start = parseInt(startArg, 10);
  const end = parseInt(endArg, 10);
  const batchCount = batchCountArg ? parseInt(batchCountArg, 10) : undefined;
  if (!Number.isFinite(start) || !Number.isFinite(end) || start < 1 || end < start) {
    console.error('Invalid start/end range. Start and end must be positive integers and end >= start.');
    process.exit(1);
  }
  try {
    inputArg = path.resolve(`./src/uploads/${inputArg}`);
    const result = await splitCompanyContactsInRange(start, end, batchCount, inputArg);
    if (result && result.data) {
      console.log('\n=== CSV Range Split Summary ===');
      for (const [k, v] of Object.entries(result.data)) {
        console.log(`${k}: ${v}`);
      }
      console.log('==============================\n');
    } else {
      console.log(result);
    }
  } catch (err) {
    console.error('Error:', err);
    process.exit(2);
  }
}

main();
