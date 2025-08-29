const express = require('express')
const router = express.Router()
const contactRoutes = require('./contact/contact.route')

router.use(contactRoutes)

module.exports = router
