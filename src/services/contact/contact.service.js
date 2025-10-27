const { sendAPIerror } = require('../../middleware/apiError')
const { contact, company } = require('../../models')
const { responseMessage } = require('../../utils/responseMessage')
const statusCode = require('../../utils/statusCode')
const { getPagination, generatePaginatedResponse, getRandomHttpProxy } = require('../../helper/common')
const { Op } = require('sequelize');
const fs = require('fs')
const path = require('path')
exports.splitCompanyContacts = async () => {
	// Stream a huge CSV and split into multiple CSV files with N records per file.
	// N is configured via env CSV_SPLIT_SIZE (default 100000). Each output file includes the header row.
	// Memory-safety: uses streaming IO with backpressure; never buffers entire file.

	let parse
	try {
		// Lazy require to keep scope local and avoid top-level changes
		;({ parse } = require('csv-parse'))
	} catch (err) {
		return sendAPIerror(
			statusCode.SERVERERROR,
			'Missing dependency: csv-parse. Please install it (npm i csv-parse) and try again.'
		)
	}

	// Resolve input CSV path (env override supported)
	const inputCsvPath = process.env.CSV_INPUT_PATH
		? path.resolve(process.env.CSV_INPUT_PATH)
		: path.resolve(__dirname, '../../uploads/test-input.csv')

	if (!fs.existsSync(inputCsvPath)) {
		console.log(`Input CSV not found at ${inputCsvPath}`);
		return sendAPIerror(statusCode.NOTFOUND, `Input CSV not found at ${inputCsvPath}`)
	}

	// Records per output file
	const chunkSize = Number.parseInt(process.env.CSV_SPLIT_SIZE || '100000', 10)
	if (!Number.isFinite(chunkSize) || chunkSize <= 0) {
		return sendAPIerror(statusCode.BADREQUEST, 'CSV_SPLIT_SIZE must be a positive integer')
	}

	// Prepare output directory
	const baseName = path.basename(inputCsvPath, path.extname(inputCsvPath))
	const timestamp = new Date()
		.toISOString()
		.replace(/[-:T]/g, '')
		.replace(/\..+/, '') // yyyymmddhhMMss
	const outputDir = process.env.CSV_OUTPUT_DIR
		? path.resolve(process.env.CSV_OUTPUT_DIR)
		: path.resolve(__dirname, `../../uploads/splits/${baseName}_${timestamp}`)

	const ensureDir = (dir) => {
		if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true })
	}
	ensureDir(outputDir)

	// CSV stringifier (minimal) – always quote, escape embedded quotes by doubling
	const escapeCsvField = (val) => {
		if (val === null || val === undefined) return '""'
		const str = String(val)
		return '"' + str.replace(/"/g, '""') + '"'
	}
	const rowToCsv = (row) => row.map(escapeCsvField).join(',') + '\n'

	// Stream state
	let headerRow = null
	let outStream = null
	let fileIndex = 0
	let recordsInCurrent = 0
	let totalRecords = 0
	let emailIndex = -1
	let rootDomainIndex = -1

	const openNextFile = () => {
    if (fileIndex === 0) {
      console.log('Splitting started... 🚀');
    }
    console.log(`Batch ${fileIndex + 1} created successfully! 🎉`);

		if (!headerRow) throw new Error('Header row not initialized')
		fileIndex += 1
		recordsInCurrent = 0
		const fileName = `${baseName}_part_${String(fileIndex).padStart(5, '0')}.csv`
		const filePath = path.join(outputDir, fileName)
    outStream = fs.createWriteStream(filePath, { encoding: 'utf8' })
    outStream.setMaxListeners(50) // Prevent MaxListenersExceededWarning for drain events
		// Write header immediately
		const headerLine = rowToCsv(headerRow)
		const ok = outStream.write(headerLine)
		if (!ok) {
			// Extremely unlikely for a single header line, but handle for completeness
			// Caller (parser) should be paused already when we use this helper in backpressure paths
			outStream.once('drain', () => {})
		}
		// Propagate stream errors
		outStream.on('error', (err) => {
			// Ensure the pipeline aborts on write errors
			readStream.destroy(err)
		})
	}

	const readStream = fs.createReadStream(inputCsvPath)
	const parser = parse({
		// We’ll manually handle headers: first row captured and copied into each chunk
		columns: false,
		bom: true,
		skip_empty_lines: true,
		relax_column_count: true,
	})

	// Build a promise around the stream pipeline for async/await ergonomics
	const result = await new Promise((resolve, reject) => {
	let fileCount = 0
	let totalRows = 0 // all data rows (excluding compliance notice and header)
	let recordsWithoutEmail = 0 // rows with no valid email
	console.log('Crunching your CSV... Grab a coffee ☕');

		// Handle backpressure by pausing parser when write buffer is full
		const writeRecord = (row, isLastEmailInRow = true) => {
			if (!outStream) {
				openNextFile()
				fileCount += 1
			}
			const line = rowToCsv(row)
			const canContinue = outStream.write(line)
			recordsInCurrent += 1
			totalRecords += 1
			// Only rotate after processing all emails for the current CSV row
			if (recordsInCurrent >= chunkSize && isLastEmailInRow) {
				parser.pause()
				const closeAndResume = () => {
					outStream.end(() => {
						outStream = null
						parser.resume()
					})
				}
				if (!canContinue) {
					outStream.once('drain', closeAndResume)
				} else {
					closeAndResume()
				}
				return
			}
			// Normal backpressure handling when not rotating
			if (!canContinue) {
				parser.pause()
				outStream.once('drain', () => parser.resume())
			}
		}

		parser.on('error', (err) => reject(err))
		readStream.on('error', (err) => reject(err))

		let skipComplianceNotice = true;
		parser.on('data', (record) => {
			// Skip the first line if it is a Compliance Notice
			if (skipComplianceNotice) {
				const firstLine = (record[0] || '').toString().trim().toLowerCase();
				if (firstLine.startsWith('compliance notice:')) {
					skipComplianceNotice = false;
					return;
				}
				skipComplianceNotice = false;
			}
			if (!headerRow) {
				headerRow = record;
				// Find the fixed 'Emails', 'Telephone', and 'root domain' column indices
				const normalized = headerRow.map((h) => String(h || '').trim().toLowerCase());
				emailIndex = normalized.findIndex((h) => h === 'emails');
				if (emailIndex === -1) {
					reject(new Error("'Emails' column not found in header"));
				}
				let rootDomainIndex = normalized.findIndex((h) => h === 'root domain');
				// Telephone column
				let telephoneIndex = normalized.findIndex((h) => h === 'telephones');
				let extraTelIndex = normalized.findIndex((h) => h === 'extra telephones');
				// If 'extra telephones' column doesn't exist, add it
				if (extraTelIndex === -1) {
					headerRow.push('extra telephones');
					extraTelIndex = headerRow.length - 1;
				}
				// Save for use in closure
				parser.telephoneIndex = telephoneIndex;
				parser.extraTelIndex = extraTelIndex;
				parser.rootDomainIndex = rootDomainIndex;
				return;
			}
			// Skip if root domain contains .gov or .edu
			rootDomainIndex = parser.rootDomainIndex;
			if (rootDomainIndex !== -1) {
				const rootDomainVal = record[rootDomainIndex];
				if (rootDomainVal && /\.gov|\.edu/i.test(String(rootDomainVal))) {
					return;
				}
			}
			totalRows++;
			// For each email in the Emails cell, create a new row with that email and same other fields
			const emailVal = record[emailIndex];
			if (emailVal === undefined || emailVal === null) {
				recordsWithoutEmail++;
				return;
			}
			const emails = String(emailVal)
				.split(';')
				.map(e => e.trim())
				.filter(e => e.length > 0);
			if (emails.length === 0) {
				recordsWithoutEmail++;
				return;
			}

			// Telephone logic
			const telephoneIndex = parser.telephoneIndex;
			const extraTelIndex = parser.extraTelIndex;
			let telVal = telephoneIndex !== -1 ? record[telephoneIndex] : '';
			let telNumbers = [];
			if (telVal !== undefined && telVal !== null) {
				telNumbers = String(telVal)
					.split(';')
					.map(t => t.replace(/^ph:/i, '').trim())
					.filter(t => t.length > 0);
			}
			let firstTel = telNumbers.length > 0 ? telNumbers[0] : '';
			let extraTels = telNumbers.length > 1 ? telNumbers.slice(1).join(';') : '';

			for (let i = 0; i < emails.length; i++) {
				// Filter out emails containing specific keywords
				const singleEmail = emails[i];
				
				const skipKeywords = [
					'service', 'customer', 'care', 'support', 'helpdesk',
					'help', 'admin', 'privacy', 'careers', 'hr',
					'shipping', 'order', 'service', 'admissions', 'team', 'auto-reply', 'do-not-reply', 'no-reply',
					'.gov', '.edu', 'dispatch', 'legal'
				];

				const emailLower = singleEmail.toLowerCase();
				const shouldSkip = skipKeywords.some(keyword => emailLower.includes(keyword));

				if (shouldSkip) {
					continue; // Skip this email
				}
				const isLastEmail = i === emails.length - 1;
				// Clone the row and set the Emails column to the single email
				const newRow = [...record];
				newRow[emailIndex] = singleEmail;
				// Set Telephone and extra telephones
				if (telephoneIndex !== -1) newRow[telephoneIndex] = firstTel;
				// Ensure extra telephones field exists
				if (extraTelIndex >= newRow.length) {
					// pad with empty fields if needed
					while (newRow.length < extraTelIndex) newRow.push('');
					newRow.push(extraTels);
				} else {
					newRow[extraTelIndex] = extraTels;
				}
				writeRecord(newRow, isLastEmail);
			}
		});

		parser.on('end', () => {
		console.log('Splitting complete! Check your output folder. ✅');
			// Gracefully close the last stream if still open
			if (outStream) {
				outStream.end()
			}
			resolve({
				fileCount,
				totalRecords,
				totalRows,
				recordsWithoutEmail
			})
		})

		// Kick off the pipeline
		readStream.pipe(parser)
	}).catch((err) => {
		return sendAPIerror(statusCode.SERVERERROR, err.message || 'Failed to split CSV')
	})

	// Write summary.txt in outputDir
	// Pretty table formatter
	const pad = (str, len) => String(str).padEnd(len, ' ');
	const summaryRows = [
		['Input', inputCsvPath],
		['Output Directory', outputDir],
		['Files Created', result.fileCount],
		['Total Records', result.totalRows],
		['Records Without Email', result.recordsWithoutEmail],
		['Records Processed', result.totalRecords],
		['Chunk Size', chunkSize],
	];
	const keyWidth = Math.max(...summaryRows.map(([k]) => k.length)) + 2;
	const valWidth = Math.max(...summaryRows.map(([,v]) => String(v).length)) + 2;
	const border = `|${'='.repeat(keyWidth + valWidth + 1)}|`;
	const title = pad('CSV Split Summary', keyWidth + valWidth + 1);
	let summaryText = `${border}\n|${title}|\n${border}\n`;
	for (const [k, v] of summaryRows) {
		summaryText += `| ${pad(k + ':', keyWidth)}${pad(v, valWidth)}|\n`;
	}
	summaryText += border + '\n';
	try {
		fs.writeFileSync(path.join(outputDir, 'summary.txt'), summaryText, 'utf8');
	} catch (e) {
		// If writing summary fails, do not block the main response
		console.error('Failed to write summary.txt:', e);
	}
	return {
		message: responseMessage('success', 'split', 'CSV'),
		data: {
			input: inputCsvPath,
			outputDir,
			filesCreated: result.fileCount,
			totalRecords: result.totalRows,
			recordsWithoutEmail: result.recordsWithoutEmail,
			recordsProcessed: result.totalRecords,
			chunkSize,
		},
	}
}



exports.splitCompanyContactsInRange = async (start, end, batchCount, inputCsvPathArg) => {
	let parse
	try {
		;({ parse } = require('csv-parse'))
	} catch (err) {
		return sendAPIerror(
			statusCode.SERVERERROR,
			'Missing dependency: csv-parse. Please install it (npm i csv-parse) and try again.'
		)
	}

	// Resolve input CSV path
	const inputCsvPath = inputCsvPathArg
		? path.resolve(inputCsvPathArg)
		: (process.env.CSV_INPUT_PATH
			? path.resolve(process.env.CSV_INPUT_PATH)
			: path.resolve(__dirname, '../../uploads/test-input.csv'))

	if (!fs.existsSync(inputCsvPath)) {
		console.log(`Input CSV not found at ${inputCsvPath}`);
		return sendAPIerror(statusCode.NOTFOUND, `Input CSV not found at ${inputCsvPath}`)
	}

	// Records per output file
	const chunkSize = batchCount ? Number.parseInt(batchCount, 10) : null;
	if (batchCount && (!Number.isFinite(chunkSize) || chunkSize <= 0)) {
		return sendAPIerror(statusCode.BADREQUEST, 'batchCount must be a positive integer')
	}

	// Prepare output directory
	const baseName = path.basename(inputCsvPath, path.extname(inputCsvPath))
	const timestamp = new Date()
		.toISOString()
		.replace(/[-:T]/g, '')
		.replace(/\..+/, '') // yyyymmddhhMMss
	const outputDir = process.env.CSV_OUTPUT_DIR
		? path.resolve(process.env.CSV_OUTPUT_DIR)
		: path.resolve(__dirname, `../../uploads/splits/${baseName}_range_${timestamp}`)

	const ensureDir = (dir) => {
		if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true })
	}
	ensureDir(outputDir)

	// CSV stringifier (minimal) – always quote, escape embedded quotes by doubling
	const escapeCsvField = (val) => {
		if (val === null || val === undefined) return '""'
		const str = String(val)
		return '"' + str.replace(/"/g, '""') + '"'
	}
	const rowToCsv = (row) => row.map(escapeCsvField).join(',') + '\n'

	// Stream state
	let headerRow = null
	let outStream = null
	let fileIndex = 0
	let recordsInCurrent = 0
	let totalRecords = 0
	let emailIndex = -1
  let rootDomainIndex = -1

	const openNextFile = () => {
		if (fileIndex === 0) {
			console.log('Splitting started... 🚀');
		}
		console.log(`Batch ${fileIndex + 1} created successfully! 🎉`);

		if (!headerRow) throw new Error('Header row not initialized')
		fileIndex += 1
		recordsInCurrent = 0
		const fileName = `${baseName}_part_${String(fileIndex).padStart(5, '0')}.csv`
		const filePath = path.join(outputDir, fileName)
		outStream = fs.createWriteStream(filePath, { encoding: 'utf8' })
		outStream.setMaxListeners(50)
		// Write header immediately
		const headerLine = rowToCsv(headerRow)
		const ok = outStream.write(headerLine)
		if (!ok) {
			outStream.once('drain', () => {})
		}
		outStream.on('error', (err) => {
			readStream.destroy(err)
		})
	}

	const readStream = fs.createReadStream(inputCsvPath)
	const parser = parse({
		columns: false,
		bom: true,
		skip_empty_lines: true,
		relax_column_count: true,
	})

	// Build a promise around the stream pipeline for async/await ergonomics
	const result = await new Promise((resolve, reject) => {
		let fileCount = 0
		let totalRows = 0 // all data rows (excluding compliance notice and header)
		let recordsWithoutEmail = 0 // rows with no valid email
		let dataRowIndex = 0 // 1-based, only data rows (not header/compliance)
		let processedRows = 0 // rows actually written (after email split)
		let finished = false;
		console.log('Crunching your CSV... Grab a coffee ☕');

		// Handle backpressure by pausing parser when write buffer is full
		const writeRecord = (row, isLastEmailInRow = true) => {
			if (!outStream) {
				openNextFile()
				fileCount += 1
			}
			const line = rowToCsv(row)
			const canContinue = outStream.write(line)
			recordsInCurrent += 1
			totalRecords += 1
			processedRows += 1
			// Only rotate after processing all emails for the current CSV row (only if chunkSize is set)
			if (chunkSize && recordsInCurrent >= chunkSize && isLastEmailInRow) {
				parser.pause()
				const closeAndResume = () => {
					outStream.end(() => {
						outStream = null
						parser.resume()
					})
				}
				if (!canContinue) {
					outStream.once('drain', closeAndResume)
				} else {
					closeAndResume()
				}
				return
			}
			// Normal backpressure handling when not rotating
			if (!canContinue) {
				parser.pause()
				outStream.once('drain', () => parser.resume())
			}
		}

		parser.on('error', (err) => reject(err))
		readStream.on('error', (err) => reject(err))

		let skipComplianceNotice = true;
		parser.on('data', (record) => {
			// if (finished) return; // ignore further data after done
			// Skip the first line if it is a Compliance Notice
			if (skipComplianceNotice) {
				const firstLine = (record[0] || '').toString().trim().toLowerCase();
				if (firstLine.startsWith('compliance notice:')) {
					skipComplianceNotice = false;
					return;
				}
				skipComplianceNotice = false;
			}
			if (!headerRow) {
				headerRow = record;
				// Find the fixed 'Emails', 'Telephone', and 'root domain' column indices
				const normalized = headerRow.map((h) => String(h || '').trim().toLowerCase());
				emailIndex = normalized.findIndex((h) => h === 'emails');
				if (emailIndex === -1) {
					reject(new Error("'Emails' column not found in header"));
				}
				// Telephone column
				let rootDomainIndex = normalized.findIndex((h) => h === 'root domain');
				let telephoneIndex = normalized.findIndex((h) => h === 'telephones');
				let extraTelIndex = normalized.findIndex((h) => h === 'extra telephones');
				if (extraTelIndex === -1) {
					headerRow.push('extra telephones');
					extraTelIndex = headerRow.length - 1;
				}
				parser.telephoneIndex = telephoneIndex;
				parser.extraTelIndex = extraTelIndex;
				parser.rootDomainIndex = rootDomainIndex;
				return;
			}
			dataRowIndex++;
			if (dataRowIndex < start) return;
			if (dataRowIndex > end) {
				// We have finished the requested range, stop everything
				finished = true;
				if (outStream) {
					outStream.end();
				}
				readStream.destroy(); // stop reading file
				parser.end && parser.end(); // for compatibility, end parser if possible
				resolve({
					fileCount,
					totalRecords: processedRows,
					totalRows,
					recordsWithoutEmail
				});
				return;
			}

			// Skip if root domain contains .gov or .edu
			rootDomainIndex = parser.rootDomainIndex;
			if (rootDomainIndex !== -1) {
				const rootDomainVal = record[rootDomainIndex];
				if (rootDomainVal && /\.gov|\.edu/i.test(String(rootDomainVal))) {
					return;
				}
			}
			
			totalRows++;
			// For each email in the Emails cell, create a new row with that email and same other fields
			const emailVal = record[emailIndex];
			if (emailVal === undefined || emailVal === null) {
				recordsWithoutEmail++;
				return;
			}
			const emails = String(emailVal)
				.split(';')
				.map(e => e.trim())
				.filter(e => e.length > 0);
			if (emails.length === 0) {
				recordsWithoutEmail++;
				return;
			}

			// Telephone logic
			const telephoneIndex = parser.telephoneIndex;
			const extraTelIndex = parser.extraTelIndex;
			let telVal = telephoneIndex !== -1 ? record[telephoneIndex] : '';
			let telNumbers = [];
			if (telVal !== undefined && telVal !== null) {
				telNumbers = String(telVal)
					.split(';')
					.map(t => t.replace(/^ph:/i, '').trim())
					.filter(t => t.length > 0);
			}
			let firstTel = telNumbers.length > 0 ? telNumbers[0] : '';
			let extraTels = telNumbers.length > 1 ? telNumbers.slice(1).join(';') : '';

			for (let i = 0; i < emails.length; i++) {
				const singleEmail = emails[i];

				// Filter out emails containing specific keywords
				const skipKeywords = [
					'service', 'customer', 'care', 'support', 'helpdesk',
					'help', 'admin', 'privacy', 'careers', 'hr',
					'shipping', 'order', 'service', 'admissions', 'team', 'auto-reply', 'do-not-reply', 'no-reply',
					'.gov', '.edu', 'dispatch', 'legal'
				];

				const emailLower = singleEmail.toLowerCase();
				const shouldSkip = skipKeywords.some(keyword => emailLower.includes(keyword));

				if (shouldSkip) {
					continue; // Skip this email
				}

				const isLastEmail = i === emails.length - 1;
				const newRow = [...record];
				newRow[emailIndex] = singleEmail;
				if (telephoneIndex !== -1) newRow[telephoneIndex] = firstTel;
				if (extraTelIndex >= newRow.length) {
					while (newRow.length < extraTelIndex) newRow.push('');
					newRow.push(extraTels);
				} else {
					newRow[extraTelIndex] = extraTels;
				}
				writeRecord(newRow, isLastEmail);
			}
		});

		parser.on('end', () => {
			if (finished) return; // already handled
			console.log('Splitting complete! Check your output folder. ✅');
			if (outStream) {
				outStream.end()
			}
			resolve({
				fileCount,
				totalRecords: processedRows,
				totalRows,
				recordsWithoutEmail
			})
		})

		readStream.pipe(parser)
	}).catch((err) => {
		return sendAPIerror(statusCode.SERVERERROR, err.message || 'Failed to split CSV')
	})

	// Write summary.txt in outputDir
	const pad = (str, len) => String(str).padEnd(len, ' ');
	const summaryRows = [
		['Input', inputCsvPath],
		['Output Directory', outputDir],
		['Files Created', result.fileCount],
		['Total Records', result.totalRows],
		['Records Without Email', result.recordsWithoutEmail],
		['Records Processed', result.totalRecords],
		['Range', `${start} to ${end} `],
		['Batch Size', chunkSize || (end - start + 1)]
	];
	const keyWidth = Math.max(...summaryRows.map(([k]) => k.length)) + 2;
	const valWidth = Math.max(...summaryRows.map(([,v]) => String(v).length)) + 2;
	const border = `|${'='.repeat(keyWidth + valWidth + 1)}|`;
	const title = pad('CSV Split Range Summary', keyWidth + valWidth + 1);
	let summaryText = `${border}\n|${title}|\n${border}\n`;
	for (const [k, v] of summaryRows) {
		summaryText += `| ${pad(k + ':', keyWidth)}${pad(v, valWidth)}|\n`;
	}
	summaryText += border + '\n';
	try {
		fs.writeFileSync(path.join(outputDir, 'summary.txt'), summaryText, 'utf8');
	} catch (e) {
		console.error('Failed to write summary.txt:', e);
	}
	return {
		message: responseMessage('success', 'split', 'CSV'),
		data: {
			input: inputCsvPath,
			outputDir,
			filesCreated: result.fileCount,
			totalRecords: result.totalRows,
			recordsWithoutEmail: result.recordsWithoutEmail,
			recordsProcessed: result.totalRecords,
			range: `${start} to ${end}`,
			batchSize: chunkSize || (end - start + 1),
		},
	}
}