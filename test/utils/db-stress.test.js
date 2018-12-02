const chai = require('chai')
const _ = require('lodash')
const assert = chai.assert
const wait = require('../helpers/wait')
const tools = require('../helpers/tools')

const txs = require('../fixtures/incomingTransactions')

const events = require('../fixtures/events')

describe('db', function () {

  let db
  const tx0 = txs[0]

  before(async function () {

    process.env.cacheDuration = 2

    db = require('../../src/utils/db')
    await db.initPg()
    await db.pg.query('truncate events_log')

  })

  describe('stress test', function () {

    let d

    it('should write ~20,000 events and retrieve them by txid', async function () {

      this.timeout(60000)
      process.env.cacheDuration = 60

      d = Date.now()
      await db.pg.query('truncate events_log')

      console.log('Generating promises...')
      let promises = []
      for (let i = 0; i < events.length; i++) {
        events[i].raw_data = {}
        promises.push(db.saveEvent(events[i]))
      }
      console.log('Saving...')
      await Promise.all(promises)
          .then(() => {
            console.log('Writing time', Date.now() - d, 'ms')
          })
    })


    it('should write ~20,000 events and retrieve them by txid, writing only in PG', async function () {

      this.timeout(60000)

      d = Date.now()
      await db.pg.query('truncate events_log')

      console.log('Generating promises...')
      let promises = []
      for (let i = 0; i < events.length; i++) {
        events[i].raw_data = {}
        promises.push(db.saveEvent(events[i], {onlyPg: 1}))
      }
      console.log('Saving...')
      await Promise.all(promises)
          .then(() => {
            console.log('Writing time', Date.now() - d, 'ms')
          })

    })


    it('should write ~20,000 events and retrieve them by txid, writing only in Redis', async function () {

      process.env.total = 0
      this.timeout(60000)

      d = Date.now()
      process.env.cacheDuration = 30

      await db.pg.query('truncate events_log')

      console.log('Generating promises...')
      let promises = []
      for (let i = 0; i < events.length; i++) {
        events[i].raw_data = {}
        promises.push(db.saveEvent(events[i], {onlyRedis: 1}))
      }
      console.log('Saving...')
      await Promise.all(promises)
          .then(() => {
            console.log('Writing time', Date.now() - d, 'ms')
          })

      promises = []
      d = Date.now()
      for (let i = 0; i < events.length; i++) {
        promises.push(db.getEventByTxID(events[i].transaction_id))
        // console.log(result)
      }
      await Promise.all(promises)
          .then(() => {
            console.log('Reading time', Date.now() - d, 'ms')
          })


      console.log('Total space in memory', process.env.total)

    })


    it('should write ~20,000 compressed events and retrieve them by txid, writing only in Redis', async function () {

      process.env.total = 0
      this.timeout(60000)

      d = Date.now()
      process.env.cacheDuration = 30

      await db.pg.query('truncate events_log')

      console.log('Generating promises...')
      let promises = []
      for (let i = 0; i < events.length; i++) {
        events[i].raw_data = {}
        promises.push(db.saveEvent(events[i], {onlyRedis: 1, compressed: 1}))
      }
      console.log('Saving...')
      await Promise.all(promises)
          .then(() => {
            console.log('Writing time', Date.now() - d, 'ms')
          })

      promises = []
      d = Date.now()
      for (let i = 0; i < events.length; i++) {
        promises.push(db.getEventByTxID(events[i].transaction_id), true)
        // console.log(result)
      }
      await Promise.all(promises)
          .then(() => {
            console.log('Reading time', Date.now() - d, 'ms')
          })

      console.log('Total space in memory', process.env.total)
    })
  })

})