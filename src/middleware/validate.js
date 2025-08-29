const { sendAPIerror } = require('../middleware/apiError')
const { validationResult } = require('express-validator')
const statusCode = require('../utils/statusCode')
const validate = (schema) => {
  return [
    ...schema,
    (req, res, next) => {
      const errors = validationResult(req)
      if (!errors.isEmpty()) {
        sendAPIerror(statusCode.BADREQUEST, 'Validation failed', errors.array())
      }
      next()
    },
  ]
}

module.exports = {
  validate,
}
