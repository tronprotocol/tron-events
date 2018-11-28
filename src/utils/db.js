const bluebird = require('bluebird')
const redis = require('redis')
const _ = require('lodash')

const config = require('../config')
const {Client} = require('pg')

bluebird.promisifyAll(redis.RedisClient.prototype)
bluebird.promisifyAll(redis.Multi.prototype)


let example = {
  transaction_id: '4a438dc54bfde07d32c3f63936a205563692ee513b1a6d41ea763a873f039775',
  result:
      {
        registerBlock: 8082,
        user: '0x7ac191e4323604ee43754b3f6b14ad087cbc4faf'
      },
  resource_Node: 'FullNode',
  result_type: {registerBlock: 'uint256', user: 'address'},
  block_timestamp: '1542679812000',
  block_number: '8080',
  event_name: 'LogBet',
  contract_address: 'TGjzXqsWtxEiKWTjnU5RSA5uCJefRCyPMM',
  event_index: '2',
  raw_data:
      {
        data: '0000000000000000000000000000000000000000000000000000000000001f92',
        topics:
            ['1cf6f3b24c5afcea9fc4c045b621e03743b6f8f796bb7a0513dc7f5374270a0e',
              '0000000000000000000000007ac191e4323604ee43754b3f6b14ad087cbc4faf']
      }
}


let resultExample = {
  block_number: 118,
  block_timestamp: 1543362000000,
  contract_address: "TA2Nwc5y6z3EHKZt6gF6SWQVY9mwwdzoU5",
  event_index: 0,
  event_name: "Transfer",
  result: {
    _from: "0x8b09646ac5d8a787873d40a3609ff75725e6f104",
    _value: "5",
    _to: "0xd653c3a56841adcce098bb2fc497a477fd9a7916"
  },
  result_type: {
    _from: "address",
    _value: "uint256",
    _to: "address"
  },
  transaction_id: "cd7b0b1ed901cee2f399def4af534d488eb9305ace3d810f1ecf612bb94455ef",
  resource_Node: "FullNode"
}

class Db {

  constructor() {
    try {
      this.redis = redis.createClient(config.redis.port, config.redis.host)
    } catch (e) {
      console.error('Redis connection failed.')
    }

    try {
      this.pg = new Client(config.pg)
      this.pg.connect()
    } catch (e) {
      console.error('PostgreSQL connection failed.')
    }
  }

  keys() {
    return {
      b: 'block_number',
      t: 'block_timestamp',
      a: 'contract_address',
      i: 'event_index',
      n: 'event_name',
      r: 'result',
      e: 'result_type',
      x: 'transaction_id',
      s: 'resource_Node'
    }
  }

  inversKeys() {
    return {
      block_number: 'b',
      block_timestamp: 't',
      contract_address: 'a',
      event_index: 'i',
      event_name: 'n',
      result : 'r',
      result_type: 'e',
      transaction_id: 'x',
      resource_Node: 's'
    }
  }

  compress(eventData, exclude = '') {
    exclude = exclude.split('|')
    const compressed  = {}
    const inverseKeys = this.inversKeys()
    for (let k in inversKeys) {
      if (!exclude[k]) {
        compressed[inverseKeys[k]] = eventData[k]
      }
    }
    return compressed
  }

  uncompress(compressedData) {
    const expanded  = {}
    const keys = this.keys()
    for (let k in keys) {
      expanded[keys[k]] = compressedData[k]
    }
    return expanded
  }

  async saveEvent(eventData) {
    return Promise.all([
      this.redis.hsetAsync(`x:${eventData.transaction_id}`, `${eventData.event_name}:${eventData.event_index}`, this.compress(eventData,'transaction_id|event_name|event_index')),
      this.redis.hsetAsync(`c:${eventData.contract_address}`, `${eventData.event_name}:${eventData.block_number}:${eventData.event_index}`, this.compress(eventData, 'contract_address|event_name|block_number|event_index'))

    ])
  }
}

module.exports = new Db

