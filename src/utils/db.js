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
      this.initPg()
    } catch (e) {
      console.error(e)
      console.error('PostgreSQL connection failed.')
    }
  }

  async initPg() {

    const log = ['Initiate database if not already initiated.']

    let result = await this.pg.query('select datname from pg_database;')
    let found = false
    for (let row of result.rows) {
      if (row.datname === 'events') {
        found = true
        log.push('Database events already exists.')
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
        log.push('Table events_log already exists')
        break
      }
    }

    if (!found) {
      await this.pg.query('create table events_log (' +
          'id serial primary key,' +
          'block_number integer not null,' +
          'block_timestamp integer not null,' +
          'contract_address varchar(34) not null,' +
          'event_index integer,' +
          'event_name  text not null,' +
          'result json not null,' +
          'result_type json not null,' +
          'transaction_id varchar(64) not null,' +
          'resource_Node varchar(20) not null' +
          ');')
      log.push('Table events_log created')
      for (let col of 'block_number|contract_address|event_name|transaction_id'.split('|')) {
        await this.pg.query(`create unique index ${col}_idx on events_log (${col});`)
        log.push(`Index ${col}_idx set`)
      }
    }

    console.log(log.join('\n'))

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
      result: 'r',
      result_type: 'e',
      transaction_id: 'x',
      resource_Node: 's'
    }
  }

  compress(eventData, exclude = []) {
    const compressed = {}
    const inverseKeys = this.inversKeys()
    for (let k in inversKeys) {
      if (!exclude[k]) {
        compressed[inverseKeys[k]] = eventData[k]
      }
    }
    return compressed
  }

  uncompress(compressedData) {
    const expanded = {}
    const keys = this.keys()
    for (let k in keys) {
      expanded[keys[k]] = compressedData[k]
    }
    return expanded
  }

  formatKey(eventData, keys) {
    let key = ''
    for (let k in keys) {
      key += (key ? ':' : '') + eventData[k]
    }
    return key
  }

  async saveEvent(eventData) {

    // caching by transaction_id

    const txKey = this.formatKey(eventData, ['transaction_id'])
    const subKey = this.formatKey(eventData, ['event_name', 'event_index'])
    const txData = this.compress(eventData, ['transaction_id', 'event_name', 'event_index'])
    const txRedisPromise = this.redis.hsetAsync(
        txKey,
        subKey,
        txData
    ).then(() => this.redis.expireAsync(txKey, 3600))

    // caching by contract_address and block_number

    const cKey = this.formatKey(eventData, ['contract_address', 'block_number'])
    let cData = this.compress(eventData, ['contract_address', 'block_number', 'event_name', 'event_index'])
    const cRedisPromise = this.redis.hsetAsync(
        cKey,
        subKey,
        cData
    ).then(() => this.redis.expireAsync(cKey, 3600))

    // saving to PostgreSQL

    const text = 'INSERT INTO events_log(block_number, block_timestamp, contract_address, event_index, event_name, result, result_type, transaction_id, resource_Node) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *'
    const values = []
    for (let key in this.inversKeys()) {
      values.push(eventData[key])
    }

    const pgPromise = client.query(text, values)

    return Promise.all([
      txRedisPromise,
      cRedisPromise,
      pgPromise
    ])
  }

  async getEventByTxID(eventData) {

    // caching by transaction_id

    const txKey = this.formatKey(eventData, ['transaction_id'])
    const txRedisPromise = this.redis.hsetAsync(
        txKey,
        subKey,
        txData
    ).then(() => this.redis.expireAsync(txKey, 3600))

  }
}

module.exports = new Db

