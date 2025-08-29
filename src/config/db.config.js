const { CONFIG } = require('./config')

const dbConfig = {
  username: CONFIG.DB.USERNAME,
  password: CONFIG.DB.PASSWORD,
  database: CONFIG.DB.NAME,
  host: CONFIG.DB.HOST,
  port: CONFIG.DB.PORT,
  dialect: 'postgres',
  logging: false,
  pool: {
    max: 5,
    min: 0,
    acquire: 120000,
    idle: 10000,
  },
  define: {
    timestamps: false,
  },
}

module.exports = { dbConfig }
