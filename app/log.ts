import { Run } from 'energy-label-types'

export async function log(run: Run) {
    console.log(`Logging run ${JSON.stringify(run)}`)
}
