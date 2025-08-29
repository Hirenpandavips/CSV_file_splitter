var fs = require('fs')
const { deleteLocalfile } = require('../helper/common')
const { dbConnection } = require('../models')
const errorLogStream = fs.createWriteStream('./logs/error.log', { flags: 'a' })

const apiHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next)
}
const apiHandlerWithTransaction = async (serviceFn) => {
  const transaction = await dbConnection.transaction()
  try {
    const result = await serviceFn(transaction)
    await transaction.commit()
    return result
  } catch (error) {
    await transaction.rollback()
    throw error
  }
}

const globalErrorHandler = (err, req, res, next) => {
  const statusCode = err.status || 500
  const errorResponse = {
    message: err.message || 'Internal Server Error',
    status: statusCode,
    path: req.originalUrl,
    timestamp: new Date().toISOString(),
  }
  if (err?.errors) {
    errorResponse.errors = err.errors
  }

  if (statusCode == 500) {
    console.log(err.stack)
    const errorMessage = `Error occurred: ${err.message}\nStack: ${err.stack}\n`
    errorLogStream.write(errorMessage)
  }

  if (req.files && Object.keys(req.files).length > 0) {
    Object.keys(req.files).forEach((fieldName) => {
      const fileOrFiles = req.files[fieldName]

      // Handle case where multiple files are uploaded (array)
      if (Array.isArray(fileOrFiles)) {
        fileOrFiles.forEach((file) => {
          deleteLocalfile(file.path)
        })
      } else {
        // Handle case where a single file is uploaded (object)
        deleteLocalfile(fileOrFiles.path)
      }
    })
  }
  res.status(statusCode).json(errorResponse)
}

module.exports = { apiHandler, globalErrorHandler, apiHandlerWithTransaction }
