const express = require('express')
const controller = require('../../controllers')
const { validate } = require('../../middleware/validate')
const { createContact, getContactById } = require('../../validations/contact/contact.validation')
const router = express.Router()


router.post('/split-company-contacts', controller.contactController.splitCompanyContacts)

module.exports = router
