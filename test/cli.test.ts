import { Cli } from '../app/cli.js'

import assert from 'assert'
import { describe, it } from 'node:test'

describe('CLI Arguments', () => {
    const args = ['--host=localhost']
    it('Can read parse arguments', () => {
        const cli = new Cli(args)
        const val = cli.default('Not specified').get('--host')
        assert.strictEqual(val, 'localhost')
    })

    it('Can set default values', () => {
        const cli = new Cli([])
        const def = '127.0.0.2'
        const val = cli.default(def).get('--host')

        assert.strictEqual(val, def)
    })
})
