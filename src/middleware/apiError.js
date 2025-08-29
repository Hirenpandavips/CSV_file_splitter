const statusCode = require('../utils/statusCode')

const errorFormatter = (errors) => {
  const formattedErrors = {}
  errors.forEach((error) => {
    formattedErrors[error.path] = error.msg
  })
  return formattedErrors
}

const sendAPIerror = (statusCode, message, errors = []) => {
  const error = new Error(message || 'Internal Server Error')
  error.status = statusCode || 500
  if (errors.length > 0) {
    error.errors = errorFormatter(errors)
  }
  throw error
}

const sendSuccess = ({ res, message, data = null }) => {
  const response = { message: message || 'success' }
  if (data) response.data = data
  res.status(statusCode.SUCCESS).send(response)
}

module.exports = {
  sendAPIerror,
  sendSuccess,
}
