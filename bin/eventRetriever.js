#!/usr/bin/env node

const fs = require('fs')
const path = require('path')
const request = require('superagent')

async function eventScrape() {

  let results = []

  let total = 100
  let count = 0

  for (let i = 0; i < total; i++) {

    await request
        .get(`https://api.trongrid.io/events?size=200&page=${i+1}`)
        .timeout(5000)
        .then(res => {
          count++
          results = results.concat(res.body)
          if (count === total)
            formatAndSave(results)
        }, err => {
          console.error('err')
        })
  }
}

function formatAndSave(results) {
  let clean = []
  let yet = {}
  for (let event of results) {
    let json = JSON.stringify(event)
    if (!yet[json]) {
      clean.push(event)
      yet[json] = 1
    } else {
      console.log('Duplicate')
    }

  }
  fs.writeFileSync(path.resolve(__dirname, '../test/fixtures/events.json'),
      JSON.stringify(clean, null, 2))
}



eventScrape()



//https://api.trongrid.io/events?size=200&page=1