const { param, body } = require('express-validator')

const createContact = [
  body('firstName')
    .notEmpty()
    .withMessage('First name is required')
    .isLength({ min: 2 })
    .withMessage('First name must be at least 2 characters'),
  body('lastName')
    .notEmpty()
    .withMessage('Last name is required')
    .isLength({ min: 2 })
    .withMessage('Last name must be at least 2 characters'),
  body('email')
    .notEmpty()
    .withMessage('Email is required')
    .isEmail()
    .withMessage('Email must be valid'),
  body('phoneNumber')
    .optional()
    .matches(/^[+]?[(]?[0-9]{3}[)]?[-\s.]?[0-9]{3}[-\s.]?[0-9]{4,6}$/)
    .withMessage('Phone number must be valid'),
  body('title').optional().isString().withMessage('Title must be a string'),
  body('companyId')
    .optional({ nullable: true })
    .isNumeric()
    .withMessage('Company ID must be a number'),
]

const getContactById = [
  param('id')
    .notEmpty()
    .withMessage('Contact ID is required')
    .isNumeric()
    .withMessage('Contact ID must be a number'),
]

module.exports = {
  createContact,
  getContactById,
}
