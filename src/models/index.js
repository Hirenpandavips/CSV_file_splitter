// 'use strict'
// const Sequelize = require('sequelize')
// const { dbConfig } = require('../config/db.config.js')
// const db = {}

// let dbConnection = new Sequelize(dbConfig.database, dbConfig.username, dbConfig.password, dbConfig)
// dbConnection
//   .authenticate()
//   .then(() => {
//     console.log('✅ Database connection established successfully.')
//   })
//   .catch((err) => {
//     console.log('Failed to connect to the database: ' + err.message)
//   })
// db.Sequelize = Sequelize
// db.dbConnection = dbConnection
// let syncDB = () => {
//   dbConnection
//     .sync({ alter: true, force: false })
//     .then(() => {
//       console.log('✅ Database synchronized successfully')
//     })
//     .catch((err) => {
//       console.log('Failed to sync db: ' + err.message)
//     })
// }
// db.syncDB = syncDB

// db.company = require('./company.js')(dbConnection, Sequelize)
// db.contact = require('./contact.js')(dbConnection, Sequelize)

// Object.keys(dbConnection.models).forEach((modelName) => {
//   if (dbConnection.models[modelName].associate) {
//     dbConnection.models[modelName].associate(dbConnection.models)
//   }
// })

// module.exports = db
