const bluebird = require('bluebird')
const redis = require('redis')
const _ = require('lodash')
const {keccak256} = require('js-sha3');

const config = require('./config')
const {Client} = require('pg')

bluebird.promisifyAll(redis.RedisClient.prototype)
bluebird.promisifyAll(redis.Multi.prototype)

const BLOCK_NUMBER = 'b'
const BLOCK_TIMESTAMP = 't'
const CONTRACT_ADDRESS = 'a'
const EVENT_INDEX = 'i'
const EVENT_NAME = 'n'
const RESULT = 'r'
const RESULT_TYPE = 'e'
const TRANSACTION_ID = 'x'
const RESOURCE_NODE = 's'
const RAW_DATA = 'w'
const HASH = 'h'

const UNCONFIRMED_PREFIX = '@'

const toCompressedKeys = {
  block_number: BLOCK_NUMBER,
  block_timestamp: BLOCK_TIMESTAMP,
  contract_address: CONTRACT_ADDRESS,
  event_index: EVENT_INDEX,
  event_name: EVENT_NAME,
  result: RESULT,
  result_type: RESULT_TYPE,
  transaction_id: TRANSACTION_ID,
  resource_Node: RESOURCE_NODE,
  raw_data: RAW_DATA
}

const returnCodes = {
  ALREADY_SET: 0,
  SET_UNCONFIRMED: 1,
  SET_CONFIRMED: 2,
  ERROR: 3
}


const toExpandedKeys = _.invert(toCompressedKeys)

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
      await this.pg.query(
          'create table events_log ( \
          id serial primary key, \
          block_number integer not null, \
          block_timestamp bigint not null, \
          contract_address varchar(34) not null, \
          event_index integer, \
          event_name  text not null, \
          result json not null, \
          result_type json not null, \
          transaction_id varchar(64) not null, \
          resource_Node varchar(20) not null, \
          raw_data json not null, \
          hash varchar(16) not null, \
          confirmed boolean default false\
          );')
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
    return toExpandedKeys
  }

  toCompressedKeys() {
    return toCompressedKeys
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

  unconfirmedSubkey(key) {
    return UNCONFIRMED_PREFIX + key
  }

  sortKeysByBlockNumberDescent(a, b) {
    const A = parseInt(a.split(':')[1])
    const B = parseInt(b.split(':')[1])
    if (A < B) return 1;
    if (A > B) return -1;
    return 0;
  }

  hashEvent(eventData) {
    return keccak256(eventData.contract_address + eventData.transaction_id + eventData.event_name + eventData.event_index).toString().substring(0, 16)
  }

  async cacheEvent(eventData, key, subKey, exclude, hash, compressed) {
    const unconfirmedSubkey = this.unconfirmedSubkey(subKey)
    const itExists = await this.redis.hgetAsync(key, subKey)
    let returnCode = returnCodes.SET_UNCONFIRMED
    if (itExists) {
      let isUnconfirmed = await this.redis.hgetAsync(key, unconfirmedSubkey)
      if (isUnconfirmed) {
        await this.redis.hdelAsync(key, unconfirmedSubkey)
        returnCode = returnCodes.SET_CONFIRMED
      } else {
        return Promise.resolve(returnCodes.ALREADY_SET)
      }
    } else {
      const event = compressed
          ? this.compress(eventData, exclude)
          : eventData
      if (hash) {
        event[HASH] = hash
      }

      {
        // used during stress tests
        !process.env.totalMemoryUsedDuringTesting || (process.env.totalMemoryUsedDuringTesting = parseInt(process.env.totalMemoryUsedDuringTesting) + event.length)
      }

      const data = {}
      data[subKey] = JSON.stringify(event)
      data[unconfirmedSubkey] = 1

      await this.redis.hmsetAsync(
          key,
          data
      )
    }
    await this.redis.expireAsync(key, process.env.cacheDuration || 3600)
    return Promise.resolve(returnCode)

  }

  async cacheEventByTxId(eventData, compressed) {

    const key = this.formatKey(eventData, ['transaction_id'])
    const subKey = this.formatKey(eventData, ['event_name', 'event_index'])

    return Promise.resolve(this.cacheEvent(eventData, key, subKey,
        ['transaction_id', 'event_name', 'event_index'], null, compressed))

  }

  async cacheEventByContractAddress(eventData, hash, compressed) {

    const key = this.formatKey(eventData, ['contract_address', 'event_name', 'block_number'])
    const subKey = this.formatKey(eventData, ['event_index'])

    return Promise.resolve(this.cacheEvent(eventData, key, subKey,
        ['contract_address', 'block_number', 'event_name', 'event_index'], hash || this.hashEvent(eventData), compressed))

  }

  async saveEvent(eventData, options = {}) {

    const hash = this.hashEvent(eventData)

    const returnCode0 = await this.cacheEventByTxId(eventData, options.compressed)
    const returnCode = await this.cacheEventByContractAddress(eventData, hash, options.compressed)

    if (returnCode0 !== returnCode) {
      // TODO Should we alert some way?
      // It is just possible that one key expired some nano seconds before the other
    }

    let text
    let values

    if (returnCode === returnCodes.SET_CONFIRMED) {
      text = 'update events_log set confirmed = true where transaction_id=$1 and block_number = $2 and event_name = $3 and event_index = $4'
      values = [eventData.transaction_id, eventData.block_number, eventData.event_name, eventData.event_index]
    } else if (returnCode === returnCodes.SET_UNCONFIRMED) {
      text = 'insert into events_log(block_number, block_timestamp, contract_address, event_index, event_name, result, result_type, transaction_id, resource_Node, raw_data, hash) values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING *'
      values = Object.keys(toCompressedKeys).map(elem => eventData[elem])
      values.push(hash)
    }

    let result
    try {
      result = !text ? false // returnCode is ALREADY_SET or ERROR
          : options.onlyRedis ? true // option for testing
              : await this.pg.query(text, values)
                  .catch(err => {
                    if (/duplicate key/.test(err.message)) {
                      return Promise.resolve('duplicate key')
                    }
                    return Promise.reject(err)
                  })
    } catch (err) {
      result = err
    }
    return Promise.resolve(result)
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

  filterEventsByConfirmation(events, onlyConfirmed) {
    let keys = []
    let unconfirmed = []
    let toBeDeleted = []
    for (let key in events) {
      if (!key.indexOf(UNCONFIRMED_PREFIX)) {
        toBeDeleted.push(key)
        unconfirmed.push(key.substring(1))
      } else {
        keys.push(key)
      }
    }
    if (onlyConfirmed) {
      toBeDeleted = toBeDeleted.concat(unconfirmed)
    }
    for (let key of toBeDeleted) {
      delete events[key]
    }
    return events
  }

  async getEventByContractAddress(address, blockNumber, eventName, size = 20, page = 1, previousLast, onlyConfirmed, isCompressed) {

    let keys = await this.redis.keys(`${address}:${blockNumber || '*' }:${eventName || '*'}`)
    keys.sort(this.sortKeysByBlockNumberDescent)
    const result = []
    let count = -1
    let nextLast = previousLast
    let started = false
    for (let i = 0; i < key.length; i++) {
      let events = this.filterEventsByConfirmation(
          await this.redis.hgetallAsync(key[i]),
          onlyConfirmed
      )
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
        let moreResult = this.getEventByContractAddressFromDB(address, blockNumber, eventName, size, page, count, nextLast)
      }
    }
    return Promise.resolve({events: result, lastEvent: nextLast})
  }

  async getEventByContractAddressFromDB(address, blockNumber, eventName, size, page, nextLast, isCompressed) {
    let text = 'select * from events_log where contract_address = $1'
    let values = [contract_address]
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

  }

}

module.exports = new Db

