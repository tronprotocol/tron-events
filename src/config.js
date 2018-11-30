if (process.env.NODE_ENV === 'test') {
  process.env.POSTGRES_PORT_5432_TCP_ADDR = process.env.REDIS_PORT_6379_TCP_ADDR = '127.0.0.1'
  process.env.POSTGRES_PORT_5432_TCP_PORT = 54320
  process.env.REDIS_PORT_6379_TCP_PORT = 63790

}

const config = {
  pg: {
    host: process.env.POSTGRES_PORT_5432_TCP_ADDR,
    port: process.env.POSTGRES_PORT_5432_TCP_PORT
  },
  redis: {
    host: process.env.REDIS_PORT_6379_TCP_ADDR,
    port: process.env.REDIS_PORT_6379_TCP_PORT
  }
}

module.exports = config