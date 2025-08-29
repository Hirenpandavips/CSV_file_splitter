exports.responseMessage = (status, action = '', module = 'Data') => {
  const messages = {
    error: `Error in ${action} data`,
    success: `${module} ${action} successfully`,
    wrong: 'Something went wrong.',
    not_found: `No such ${action} exists`,
    already_exists: `${action} already exists`,
  }
  return messages[status] || status || 'No message'
}
