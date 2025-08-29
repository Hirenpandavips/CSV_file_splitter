const fs = require('fs')

function getPagination(queryParams, defaultLimit = 10) {
  const page = parseInt(queryParams.page) || 1
  const limit = parseInt(queryParams.limit) || defaultLimit
  const offset = (page - 1) * limit

  let order = []
  if (queryParams.sortBy && queryParams.order) {
    order = [[queryParams.sortBy, queryParams.order.toUpperCase()]]
  }

  return {
    limit,
    offset,
    order,
  }
}

const generatePaginatedResponse = (data, paginationOptions, page) => {
  return {
    result: data.rows,
    pagination: {
      totalItems: data.count,
      totalPages: Math.ceil(data.count / paginationOptions.limit) || 0,
      currentPage: page,
    },
  }
}

const deleteLocalfile = (path) => {
  try {
    if (fs.existsSync(path)) {
      fs.unlinkSync(path)
    }
  } catch (error) {
    console.log(error)
  }
}

const createDirIfNotExists = (dirPath) => {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true })
  }
}


module.exports = {
  getPagination,
  generatePaginatedResponse,
  deleteLocalfile,
  createDirIfNotExists,
}
