const express = require('express')
const router = express.Router()
const utils = require('../utils')
const db = require('../utils/db')

router.get('/init-database', async function (req, res) {

  try {

    const log = []

    let result = await db.pg.query('select datname from pg_database;')
    let found = false
    for (let row of result.rows) {
      if (row.datname === 'events') {
        found = true
        log.push('Database events already exists.')
        break
      }
    }

    if (!found) {
      await db.pg.query('create database events;')
      log.push('Database events created.')

    }

    result = await db.pg.query('select * from pg_catalog.pg_tables;')

    found = false
    for (let row of result.rows) {
      if (row.tablename === 'events_log') {
        found = true
        log.push('Table events_log already exists')
        break
      }
    } //8Fcne8sHpf5nWfp

    if (!found) {
      await db.pg.query('create table events_log (' +
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
        await db.pg.query(`create unique index ${col}_idx on events_log (${col});`)
        log.push(`Index ${col}_idx set`)
      }
    }

    res.json({
      success: true,
      log
    });
    // await db.pg.end()

  } catch (err) {
    console.error(err)
    res.json({
      success: false,
      error: err.message
    })
    // await db.pg.end()
  }
})


let example = `{"transaction_id":"4a438dc54bfde07d32c3f63936a205563692ee513b1a6d41ea763a873f039775","result":"{\\"registerBlock\\":8082,\\"user\\":\\"0x7ac191e4323604ee43754b3f6b14ad087cbc4faf\\"}","resource_Node":"FullNode","result_type":"{\\"registerBlock\\":\\"uint256\\",\\"user\\":\\"address\\"}","block_timestamp":"1542679812000","block_number":"8080","event_name":"LogBet","contract_address":"TGjzXqsWtxEiKWTjnU5RSA5uCJefRCyPMM","event_index":"2","raw_data":"{\\"data\\":\\"0000000000000000000000000000000000000000000000000000000000001f92\\",\\"topics\\":[\\"1cf6f3b24c5afcea9fc4c045b621e03743b6f8f796bb7a0513dc7f5374270a0e\\",\\"0000000000000000000000007ac191e4323604ee43754b3f6b14ad087cbc4faf\\"]}"}`


router.post('/add-event', async function (req, res) {

  let key = req.body.key
  let eventData = req.body.data
  try {
    eventData = utils.parseString(eventData)
  } catch(err) {
    res.json({
      success: false,
      error: 'Malformed event data'
    })
  }

  const result = await db.saveEvent(eventData)
  res.json({
    success: true,
    result
  })

})

router.get('/get-event', async function (req, res) {

  // /event/contract/TBarNgzEngXaxMp6sKmCNyne3vGP515rJm/Transfer


})

router.get('/', async function (req, res) {
  res.json({
    name: 'TronEvents API',
    version: require('../../package').version
  })
})

module.exports = router
