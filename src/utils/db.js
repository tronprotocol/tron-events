const bluebird = require('bluebird')
const redis = require('redis')
const _ = require('lodash')
const {keccak256} = require('js-sha3');

const config = require('../config')
const {Client} = require('pg')

bluebird.promisifyAll(redis.RedisClient.prototype)
bluebird.promisifyAll(redis.Multi.prototype)

// let exampleString = `{"transaction_id":"4a438dc54bfde07d32c3f63936a205563692ee513b1a6d41ea763a873f039775","result":"{\\"registerBlock\\":8082,\\"user\\":\\"0x7ac191e4323604ee43754b3f6b14ad087cbc4faf\\"}","resource_Node":"FullNode","result_type":"{\\"registerBlock\\":\\"uint256\\",\\"user\\":\\"address\\"}","block_timestamp":"1542679812000","block_number":"8080","event_name":"LogBet","contract_address":"TGjzXqsWtxEiKWTjnU5RSA5uCJefRCyPMM","event_index":"2","raw_data":"{\\"data\\":\\"0000000000000000000000000000000000000000000000000000000000001f92\\",\\"topics\\":[\\"1cf6f3b24c5afcea9fc4c045b621e03743b6f8f796bb7a0513dc7f5374270a0e\\",\\"0000000000000000000000007ac191e4323604ee43754b3f6b14ad087cbc4faf\\"]}"}`
//
//
// let example = {
//   transaction_id: '4a438dc54bfde07d32c3f63936a205563692ee513b1a6d41ea763a873f039775',
//   result:
//       {
//         registerBlock: 8082,
//         user: '0x7ac191e4323604ee43754b3f6b14ad087cbc4faf'
//       },
//   resource_Node: 'FullNode',
//   result_type: {registerBlock: 'uint256', user: 'address'},
//   block_timestamp: '1542679812000',
//   block_number: '8080',
//   event_name: 'LogBet',
//   contract_address: 'TGjzXqsWtxEiKWTjnU5RSA5uCJefRCyPMM',
//   event_index: '2',
//   raw_data:
//       {
//         data: '0000000000000000000000000000000000000000000000000000000000001f92',
//         topics:
//             ['1cf6f3b24c5afcea9fc4c045b621e03743b6f8f796bb7a0513dc7f5374270a0e',
//               '0000000000000000000000007ac191e4323604ee43754b3f6b14ad087cbc4faf']
//       }
// }
//
//
// let resultExample = {
//   block_number: 118,
//   block_timestamp: 1543362000000,
//   contract_address: "TA2Nwc5y6z3EHKZt6gF6SWQVY9mwwdzoU5",
//   event_index: 0,
//   event_name: "Transfer",
//   result: {
//     _from: "0x8b09646ac5d8a787873d40a3609ff75725e6f104",
//     _value: "5",
//     _to: "0xd653c3a56841adcce098bb2fc497a477fd9a7916"
//   },
//   result_type: {
//     _from: "address",
//     _value: "uint256",
//     _to: "address"
//   },
//   transaction_id: "cd7b0b1ed901cee2f399def4af534d488eb9305ace3d810f1ecf612bb94455ef",
//   resource_Node: "FullNode"
// }

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
      console.error(e)
      console.error('PostgreSQL connection failed.')
    }
  }

  async initPg() {

    const log = []

    let result = await this.pg.query('select datname from pg_database;')
    let found = false
    for (let row of result.rows) {
      if (row.datname === 'events') {
        found = true
        log.push('Database events previously initiated.')
        break
      }
    }

    if (!found) {
      await this.pg.query('create database events;')
      log.push('Database events created.')

    }

    result = await this.pg.query('select * from pg_catalog.pg_tables;')

    found = false
    for (let row of result.rows) {
      if (row.tablename === 'events_log') {
        found = true
        log.push('Table events_log previously created.')
        break
      }
    }

    if (!found) {
      await this.pg.query('create table events_log (' +
          'id serial primary key,' +
          'block_number integer not null,' +
          'block_timestamp bigint not null,' +
          'contract_address varchar(34) not null,' +
          'event_index integer,' +
          'event_name  text not null,' +
          'result json not null,' +
          'result_type json not null,' +
          'transaction_id varchar(64) not null,' +
          'resource_Node varchar(20) not null,' +
          'raw_data json not null,' +
          'hash varchar(16) not null' +
          ');')
      log.push('Table events_log created')
      for (let col of 'block_number|contract_address|event_name|transaction_id'.split('|')) {
        await this.pg.query(`create index ${col}_idx on events_log (${col});`)
        log.push(`Index ${col}_idx set.`)
      }
      await this.pg.query('create unique index event_idx on events_log (block_number, contract_address, event_name, event_index, transaction_id);')
    }
    return Promise.resolve(log)
  }

  toExpandedKeys() {
    return {
      b: 'block_number',
      t: 'block_timestamp',
      a: 'contract_address',
      i: 'event_index',
      n: 'event_name',
      r: 'result',
      e: 'result_type',
      x: 'transaction_id',
      s: 'resource_Node',
      w: 'raw_data'
    }
  }

  toCompressedKeys() {
    return {
      block_number: 'b',
      block_timestamp: 't',
      contract_address: 'a',
      event_index: 'i',
      event_name: 'n',
      result: 'r',
      result_type: 'e',
      transaction_id: 'x',
      resource_Node: 's',
      raw_data: 'w'
    }
  }

  compress(eventData, exclude = []) {
    const compressed = {}
    const toCompressedKeys = this.toCompressedKeys()
    for (let k in toCompressedKeys) {
      if (!~exclude.indexOf(k)) {
        compressed[toCompressedKeys[k]] = eventData[k]
      }
    }
    return compressed
  }

  uncompress(compressedData) {
    const expanded = {}
    const keys = this.toExpandedKeys()
    if (typeof compressedData === 'string') {
      compressedData = JSON.parse(compressedData)
    }
    for (let k in keys) {
      expanded[keys[k]] = compressedData[k]
    }
    return expanded
  }

  formatKey(eventData, keys) {
    let key = ''
    for (let k of keys) {
      key += (key ? ':' : '') + eventData[k]
    }
    return key
  }

  sortKeysByBlockNumberDescent(a, b) {
    const A = parseInt(a.split(':')[1])
    const B = parseInt(b.split(':')[1])
    if (A < B) return 1;
    if (A > B) return -1;
    return 0;
  }

  async cacheEventByTxId(eventData, compressed) {
    const key = this.formatKey(eventData, ['transaction_id'])
    const subKey = this.formatKey(eventData, ['event_name', 'event_index'])
    const data = compressed
        ? JSON.stringify(this.compress(eventData, ['transaction_id', 'event_name', 'event_index']))
        : JSON.stringify(eventData)
    !process.env.total || (process.env.total = parseInt(process.env.total) + data.length)
    return this.redis.hsetAsync(
        key,
        subKey,
        data
    ).then(() => this.redis.expireAsync(key, process.env.cacheDuration || 3600))
  }

  hashEvent(eventData) {
    return keccak256(eventData.contract_address + eventData.transaction_id + eventData.event_name + eventData.event_index).toString().substring(16)
  }

  async cacheEventByContractAddress(eventData, h, compressed) {
    const key = this.formatKey(eventData, ['contract_address', 'event_name', 'block_number'])
    const subKey = this.formatKey(eventData, ['event_index'])
    const data = compressed
        ? JSON.stringify(this.compress(eventData, ['contract_address', 'block_number', 'event_name', 'event_index']))
        : JSON.stringify(eventData)
    data.h = h || this.hashEvent(eventData)
    return this.redis.hsetAsync(
        key,
        subKey,
        data
    ).then(() => this.redis.expireAsync(key, process.env.cacheDuration || 3600))
  }

  async saveEvent(eventData, options = {}) {

    // TODO handle events from fullNode and confirmed events from solidityNode

    const h = this.hashEvent(eventData)

    const text = 'INSERT INTO events_log(block_number, block_timestamp, contract_address, event_index, event_name, result, result_type, transaction_id, resource_Node, raw_data, hash) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING *'

    const values = Object.keys(this.toCompressedKeys()).map(elem => eventData[elem])
    values.push(h)

    return Promise.all([
      options.onlyPg ? null : this.cacheEventByTxId(eventData, options.compressed),
      options.onlyPg ? null : this.cacheEventByContractAddress(eventData, h, options.compressed),
      options.onlyRedis ? null : this.pg.query(text, values)
          .catch(err => {
            if (/duplicate key/.test(err.message)) {
              return Promise.resolve()
            }
            return Promise.reject(err)
          })
    ])
  }

  async getEventByTxIDFromCache(txId, isCompressed) {

    let data = await this.redis.hgetallAsync(`${txId}`)
    let lastHash
    if (data) {
      const result = []
      for (let key in data) {
        let event = data[key]
        if (isCompressed) {
          event = this.uncompress(event)
          event.transaction_id = txId
          key = key.split(':')
          event.event_name = key[0]
          event.event_index = parseInt(key[1])
        }
        result.push(event)
      }
      return Promise.resolve({events: result.join(',')})
    } else {
      return this.getEventByTxIDFromDB(txId, isCompressed)
    }
  }

  async getEventByTxIDFromDB(txId) {

    let eventData = await this.pg.query('select * from events_log where transaction_id = $1;', [txId]).rows[0]
    // Should we cache it?
    // this.cacheEventByTxId(eventData, options.compressed)
    return Promise.resolve(eventData)

  }

  async getEventByContractAddressFromCache(address, blockNumber, eventName, size = 20, previousLast, isCompressed) {

    let keys = await this.redis.keys(`${address}:${blockNumber || '*' }:${eventName || '*'}`)
    keys.sort(this.sortKeysByBlockNumberDescent)
    const result = []
    let count = -1
    let nextLast = previousLast
    let started = false
    for (let i = 0; i < key.length; i++) {
      let events = await this.redis.hgetallAsync(key[i])
      for (let eventIndex in events) {
        count++
        let event = events[eventIndex]
        if (!started) {
          if (!previousLast) {
            started = true
          } else if (previousLast === event.h) {
            started = true
            continue
          } else {
            continue
          }
        }
        if (isCompressed) {
          let [ca, bn, en] = key.split(':')
          event = this.uncompress(event)
          event.contract_address = ca
          event.block_number = bn
          event.event_name = en
          event.event_index = eventIndex
          nextLast = event.h
          delete event.h
        }
        result.push(event)
        if (count >= startAt + size) {
          break
        }
      }
      if (count < size) {
        let moreResult = this.getEventByContractAddressFromDB(address, blockNumber, eventName, size - count, nextLast)
      }
    }
    return Promise.resolve({ events: result, lastEvent: nextLast })
  }

  async getEventByContractAddressFromDB(address, blockNumber, eventName, size, nextLast, isCompressed) {
    let text = 'select * from events_log where contract_address = $1'
    let values = [ contract_address]
    if (blockNumber) {
      text += ' and block_number = $2 '
      values.push(blockNumber)
    }
    if (eventName) {
      text += ' and event_name = $' + (blockNumber ? 3 : 2)
      values.push(eventName)
    }

    let eventsData = await this.pg.query(text, values)
    // TODO find the data, optimize starting from some column

    return Promise.resolve(eventData)

  }

}

module.exports = new Db

