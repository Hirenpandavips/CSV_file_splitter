#!/usr/bin/env node
// CLI for splitting CSV files using the same logic as the API
const path = require('path');
const dotenv = require('dotenv');
dotenv.config({ path: path.join(__dirname, '.env') });


const { splitCompanyContacts, splitCompanyContactsInRange } = require('./src/services/contact/contact.service');


function parseArgs(argv) {
  const args = { _: [] };
  for (let i = 2; i < argv.length; ++i) {
    const arg = argv[i];
    if (arg.startsWith('--range=')) {
      args.range = arg.replace('--range=', '');
    } else if (arg.startsWith('--batch=')) {
      args.batch = arg.replace('--batch=', '');
    } else if (!arg.startsWith('--')) {
      args._.push(arg);
    }
  }
  return args;
}

async function main() {
  const args = parseArgs(process.argv);
  const inputArg = args._[0];
  if (!inputArg) {
    console.error('Usage: node csv-split.js <input.csv> [--range=START:END] [--batch=SIZE]');
    process.exit(1);
  }
  // const inputPath = path.resolve(`./src/uploads/${inputArg}`);
  const inputPath = path.resolve(inputArg);
  if (args.range) {
    // Range mode
    const match = args.range.match(/^(\d+):(\d+)$/);
    if (!match) {
      console.error('Invalid --range format. Use --range=START:END');
      process.exit(1);
    }
    const start = parseInt(match[1], 10);
    const end = parseInt(match[2], 10);
    const batchCount = args.batch ? parseInt(args.batch, 10) : undefined;
    if (!Number.isFinite(start) || !Number.isFinite(end) || start < 1 || end < start) {
      console.error('Invalid range. Start and end must be positive integers and end >= start.');
      process.exit(1);
    }
    try {
      const result = await splitCompanyContactsInRange(start, end, batchCount, inputPath);
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
      // console.error('Error:', err);
      process.exit(2);
    }
  } else {
    // Normal mode
    process.env.CSV_INPUT_PATH = inputPath;
    if (args.batch) {
      process.env.CSV_SPLIT_SIZE = args.batch;
    }
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
      // console.error('Error:', err);
      process.exit(2);
    }
  }
}

main();
