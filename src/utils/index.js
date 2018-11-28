
const utils = {

  parseString: function (data) {
    data = JSON.parse(data)
    for (let prop of 'result:raw_data:result_type'.split(':')) {
      if (typeof data[prop] === 'string') {
        data[prop] = JSON.parse(data[prop])
      }
    }
    return data
  }

}

module.exports = utils