import Fastify from 'fastify'
import cli from './cli.js'

export async function main() {
    const port = Number(cli.default('3000').get('--port'))
    const host = cli.default('localhost').get('--host')

    const app = Fastify({ logger: true })
    app.get('/', async () => ({ hello: 'world' }))

    try {
        await app.listen({ host: host, port: port })
    } catch (err) {
        app.log.error(err)
        process.exit(1)
    }
}
