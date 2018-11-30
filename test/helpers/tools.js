const fields = [
  'block_number',
  'block_timestamp',
  'contract_address',
  'event_index',
  'event_name',
  'result',
  'result_type',
  'transaction_id',
  'resource_Node',
  'raw_data'
]

const jsonFields = [
  'result',
  'result_type',
  'raw_data'
]

class Tools {

  stringifyEvent(event) {
    for (let key in event) {
      if (!!~jsonFields.indexOf(key)) {
        event[key] = JSON.stringify(event[key])
      }
    }
    return JSON.stringify(event)
  }

}


module.exports = new Tools