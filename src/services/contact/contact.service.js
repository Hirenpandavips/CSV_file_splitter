const { sendAPIerror } = require('../../middleware/apiError')
const { contact, company } = require('../../models')
const { responseMessage } = require('../../utils/responseMessage')
const statusCode = require('../../utils/statusCode')
const { getPagination, generatePaginatedResponse, getRandomHttpProxy } = require('../../helper/common')
const { Op } = require('sequelize');

exports.splitCompanyContacts = async () => {
	// Stream a huge CSV and split into multiple CSV files with N records per file.
	// N is configured via env CSV_SPLIT_SIZE (default 100000). Each output file includes the header row.
	// Memory-safety: uses streaming IO with backpressure; never buffers entire file.
	const fs = require('fs')
	const path = require('path')

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
		: path.resolve(__dirname, '../../uploads/main_file.csv')

	if (!fs.existsSync(inputCsvPath)) {
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

	const openNextFile = () => {
		if (!headerRow) throw new Error('Header row not initialized')
		fileIndex += 1
		recordsInCurrent = 0
		const fileName = `${baseName}_part_${String(fileIndex).padStart(5, '0')}.csv`
		const filePath = path.join(outputDir, fileName)
		outStream = fs.createWriteStream(filePath, { encoding: 'utf8' })
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

		// Handle backpressure by pausing parser when write buffer is full
		const writeRecord = (row) => {
			if (!outStream) {
				openNextFile()
				fileCount += 1
			}
			const line = rowToCsv(row)
			const canContinue = outStream.write(line)
			if (!canContinue) {
				// Pause parser until the write stream drains
				parser.pause()
				outStream.once('drain', () => parser.resume())
			}
			recordsInCurrent += 1
			totalRecords += 1
			if (recordsInCurrent >= chunkSize) {
				// Rotate file
				outStream.end()
				outStream = null
			}
		}

		parser.on('error', (err) => reject(err))
		readStream.on('error', (err) => reject(err))

				parser.on('data', (record) => {
					if (!headerRow) {
						headerRow = record
						// Find the fixed 'Emails' column index
						const normalized = headerRow.map((h) => String(h || '').trim().toLowerCase())
            emailIndex = normalized.findIndex((h) => h === 'work email')
						if (emailIndex === -1) {
							reject(new Error("'Emails' column not found in header"))
						}
						return
					}
					// Filter: only include rows where 'Emails' cell has a non-empty value
					const emailVal = record[emailIndex]
					const hasEmail = emailVal !== undefined && emailVal !== null && String(emailVal).trim().length > 0
					if (!hasEmail) return // skip this row
					writeRecord(record)
				})

		parser.on('end', () => {
			// Gracefully close the last stream if still open
			if (outStream) {
				outStream.end()
			}
			resolve({ fileCount, totalRecords })
		})

		// Kick off the pipeline
		readStream.pipe(parser)
	}).catch((err) => {
		return sendAPIerror(statusCode.SERVERERROR, err.message || 'Failed to split CSV')
	})

	return {
		message: responseMessage('success', 'split', 'CSV'),
		data: {
			input: inputCsvPath,
			outputDir,
			filesCreated: result.fileCount,
			recordsProcessed: result.totalRecords,
			chunkSize,
		},
	}
}
