const services = require('../../services')
const { sendSuccess } = require('../../middleware/apiError')
const { apiHandler } = require('../../middleware/globalErrorHandler')


exports.splitCompanyContacts = apiHandler(async (req, res) => {
  const result = await services.contactService.splitCompanyContacts()
  sendSuccess({ res, ...result })
})
