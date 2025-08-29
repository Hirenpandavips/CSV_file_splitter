/* eslint-disable no-undef */
const CONFIG = {
  NODE_ENV: process.env.NODE_ENV || 'development',
  DB: {
    NAME: process.env.DB_NAME,
    USERNAME: process.env.DB_USERNAME,
    PASSWORD: process.env.DB_PASSWORD,
    HOST: process.env.HOST,
    PORT: process.env.DB_PORT ? parseInt(process.env.DBPORT) : 5432,
  },
  PORT: process.env.PORT ? parseInt(process.env.PORT) : 4000,
}

module.exports = { CONFIG }
