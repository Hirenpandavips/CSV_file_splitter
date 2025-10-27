const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const AdmZip = require('adm-zip');

// Configuration
const ROOT_FOLDER = process.argv[2] || './data'; // Root folder containing country folders
const BATCH_SIZE = process.argv[3] || process.env.CSV_SPLIT_SIZE || '50000';
const DELETE_AFTER_PROCESSING = process.env.DELETE_AFTER_PROCESSING === 'true'; // Optional: delete CSV after processing

console.log('🚀 Starting batch CSV processor...\n');
console.log(`Root Folder: ${ROOT_FOLDER}`);
console.log(`Batch Size: ${BATCH_SIZE}\n`);

// Recursively find all zip files
function findZipFiles(dir) {
  const zipFiles = [];
  const items = fs.readdirSync(dir);

  for (const item of items) {
    const fullPath = path.join(dir, item);
    const stat = fs.statSync(fullPath);

    if (stat.isDirectory()) {
      zipFiles.push(...findZipFiles(fullPath));
    } else if (path.extname(item).toLowerCase() === '.zip') {
      zipFiles.push(fullPath);
    }
  }

  return zipFiles;
}

// Extract zip file to same directory and return newly extracted CSV files
function extractZip(zipPath) {
  console.log(`📦 Extracting: ${zipPath}`);
  const zip = new AdmZip(zipPath);
  const extractPath = path.dirname(zipPath);

  // Get existing CSV files before extraction
  const existingCsvFiles = new Set(findCsvFiles(extractPath).map(f => path.basename(f)));

  try {
    zip.extractAllTo(extractPath, true);
    console.log(`✅ Extracted to: ${extractPath}\n`);
    
    // Get CSV files after extraction and filter only new ones
    const allCsvFiles = findCsvFiles(extractPath);
    const newCsvFiles = allCsvFiles.filter(f => !existingCsvFiles.has(path.basename(f)));
    
    return { extractPath, newCsvFiles };
  } catch (error) {
    console.error(`❌ Failed to extract ${zipPath}: ${error.message}\n`);
    return null;
  }
}

// Find CSV files in extracted folder
function findCsvFiles(dir) {
  const csvFiles = [];
  const items = fs.readdirSync(dir);

  for (const item of items) {
    const fullPath = path.join(dir, item);
    const stat = fs.statSync(fullPath);

    if (stat.isFile() && path.extname(item).toLowerCase() === '.csv') {
      csvFiles.push(fullPath);
    }
  }

  return csvFiles;
}

// Process single CSV file
function processCsvFile(csvPath) {
  console.log(`📊 Processing CSV: ${csvPath}`);

  try {
    const command = `node csv-split.js "${csvPath}" --batch=${BATCH_SIZE}`;
    const output = execSync(command, {
      cwd: __dirname,
      encoding: 'utf8',
      stdio: 'inherit' // Show real-time output
    });

    console.log(`✅ Completed: ${csvPath}\n`);

    // if (DELETE_AFTER_PROCESSING) {
    //   fs.unlinkSync(csvPath);
    //   console.log(`🗑️  Deleted: ${csvPath}\n`);
    // }

    return true;
  } catch (error) {
    console.error(`❌ Failed to process ${csvPath}: ${error.message}\n`);
    return false;
  }
}

// Main execution
async function main() {
  const startTime = Date.now();
  let totalZips = 0;
  let totalCsvs = 0;
  let successCount = 0;
  let failCount = 0;

  // Check if root folder exists
  if (!fs.existsSync(ROOT_FOLDER)) {
    console.error(`❌ Root folder not found: ${ROOT_FOLDER}`);
    process.exit(1);
  }

  // Find all zip files
  console.log('🔍 Scanning for zip files...\n');
  const zipFiles = findZipFiles(ROOT_FOLDER);
  totalZips = zipFiles.length;

  if (zipFiles.length === 0) {
    console.log('⚠️  No zip files found.');
    return;
  }

  console.log(`Found ${zipFiles.length} zip file(s)\n`);
  console.log('─'.repeat(60) + '\n');

  // Process each zip file
  for (let i = 0; i < zipFiles.length; i++) {
    const zipPath = zipFiles[i];
    const countryFolder = path.basename(path.dirname(zipPath));

    console.log(`[${i + 1}/${zipFiles.length}] Country: ${countryFolder}`);
    console.log(`Zip: ${path.basename(zipPath)}\n`);

    // Extract zip
    const extractResult = extractZip(zipPath);
    if (!extractResult) {
      failCount++;
      continue;
    }

    const { extractPath, newCsvFiles } = extractResult;
    const csvFiles = newCsvFiles; // Only process newly extracted CSV files
    totalCsvs += csvFiles.length;

    if (csvFiles.length === 0) {
      console.log('⚠️  No CSV files found in zip\n');
      continue;
    }

    console.log(`Found ${csvFiles.length} CSV file(s) to process\n`);

    // Process each CSV file
    for (let j = 0; j < csvFiles.length; j++) {
      const csvPath = csvFiles[j];
      console.log(`  [${j + 1}/${csvFiles.length}] ${path.basename(csvPath)}`);

      const success = processCsvFile(csvPath);
      if (success) {
        successCount++;
      } else {
        failCount++;
      }
    }

    console.log('─'.repeat(60) + '\n');
  }

  // Summary
  const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
  console.log('🎉 Batch processing complete!\n');
  console.log('Summary:');
  console.log(`  Zip Files Processed: ${totalZips}`);
  console.log(`  CSV Files Found: ${totalCsvs}`);
  console.log(`  Successfully Processed: ${successCount}`);
  console.log(`  Failed: ${failCount}`);
  console.log(`  Total Time: ${duration} minutes`);
}

// Run
main().catch(error => {
  console.error('❌ Fatal error:', error);
  process.exit(1);
});