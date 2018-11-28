
const config = {
  pg: {
    host: process.env.POSTGRES_PORT_5432_TCP_ADDR,
    port: process.env.POSTGRES_PORT_5432_TCP_PORT,
  },
  redis: {
    host: process.env.REDIS_PORT_6379_TCP_ADDR,
    port: process.env.REDIS_PORT_6379_TCP_PORT
  }
}

module.exports = config