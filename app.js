/* eslint-disable no-undef */
const express = require('express')
const path = require('path')
const dotenv = require('dotenv')
dotenv.config({ path: path.join(__dirname, '/.env') })
const cors = require('cors')

const compression = require('compression')
const app = express()
const router = require('./src/routes')
const { globalErrorHandler } = require('./src/middleware/globalErrorHandler')
var morgan = require('morgan')
const { rateLimit } = require('express-rate-limit')
var fs = require('fs')
const { createDirIfNotExists } = require('./src/helper/common')
const statusCode = require('./src/utils/statusCode')
const { logUrl } = require('./src/middleware/url-logger.middleware')

const limiter = rateLimit({
  windowMs: 1 * 60 * 1000, // 1 minutes
  max: 200, // Limit each IP to 50 request per 10 min
  message: 'Too many requests from this IP, please try again later.',
})
app.use(limiter)

app.use(compression())

// Middleware for parsing application/json and application/x-www-form-urlencoded
app.use(express.json())
app.use(express.urlencoded({ extended: true }))

//for CORS
app.use(cors())

// Storing logs when errors are encountered
const logDir = path.join(__dirname, 'logs')
createDirIfNotExists(logDir)
const errorLogStream = fs.createWriteStream(logDir, { flags: 'a' })
app.use(
  morgan('combined', {
    skip: function (req, res) {
      return res.statusCode < 500 // Skip all status codes less than 500
    },
    stream: errorLogStream,
  })
)

// url logger
app.use(logUrl)

// Routes
app.use('/api', router)

app.use('*', (req, res) => {
  res.status(statusCode.NOTFOUND).send({ message: 'Invalid endpoint' })
})

// Global error handler middleware
app.use(globalErrorHandler)

module.exports = app
