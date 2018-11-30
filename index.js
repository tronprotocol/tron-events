const express = require('express')
const morgan = require('morgan')
const chalk = require('chalk')
const _ = require('lodash')
const api = require('./src/routes/api')
const config = require('./src/config')
const bodyParser = require('body-parser')

process.on('uncaughtException', function (error) {
  console.error(error.message)
})

const only = () => {
  return function (tokens, req, res) {
    const status = tokens.status(req, res)
    const color = status < 400 ? 'green' : 'red'
    return chalk[color]([
      tokens.method(req, res),
      tokens.url(req, res),
      status,
      tokens.res(req, res, 'content-length'), '-',
      tokens['response-time'](req, res), 'ms'
    ].join(' '))
  }
}

const app = express();
app.use(morgan(only()))
app.use(bodyParser.json())

app.use('/api', api)

app.use('/favicon.ico', function (req, res) {
  res.send('')
})

app.use('/', function (req,res) {
  res.send('Hello world!')
})


app.listen(8060);

exports.app = app

const n = "\n"

console.log('TronEvents listening on', chalk.bold('http://127.0.0.1:8060'))

