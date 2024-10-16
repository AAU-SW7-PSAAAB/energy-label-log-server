import Fastify from 'fastify'

export async function main() {
    const app = Fastify({ logger: true })
    app.get('/', async () => ({ hello: 'world' }))

    try {
        await app.listen({ port: 3000 })
    } catch (err) {
        app.log.error(err)
        process.exit(1)
    }
}
