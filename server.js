/* eslint-disable no-undef */
const app = require('./app')
const http = require('http')
// const db = require('./src/models/index')
const { CONFIG } = require('./src/config/config')
// const { syncDB } = db
const port = CONFIG.PORT
let httpServer
httpServer = http.createServer(app)

// Listen on provided port, on all network interfaces.
httpServer.listen(port)
httpServer.on('error', onError)
httpServer.on('listening', onListening)

function onError(error) {
  if (error.syscall !== 'listen') throw error
  let bind = typeof port === 'string' ? 'Pipe ' + port : 'Port ' + port

  // handle specific listen errors with friendly messages
  switch (error.code) {
    case 'EACCES':
      console.error(bind + ' requires elevated privileges')
      process.exit(1)
      break
    case 'EADDRINUSE':
      console.error(bind + ' is already in use')
      process.exit(1)
      break
    default:
      throw error
  }
}

// Event listener for server "listening" event.
function onListening() {
  let addr = httpServer.address()
  const bind = typeof addr === 'string' ? `pipe ${addr}` : `port ${addr.port}`
  console.log(`🚀 Server is running on ${bind}`)
  // syncDB()
}

// Uncaught exception handler
process.on('uncaughtException', (reason, promise) => {
  console.error('Uncaught Exception: at:', promise, 'reason:', reason)
})

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason)
})
