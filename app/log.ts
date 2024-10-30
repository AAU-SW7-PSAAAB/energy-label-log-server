import { Run } from 'energy-label-types'
import cli from './cli.js'
import mariadb from 'mariadb'

export async function log(run: Run) {
    console.log(`Loggin run ${JSON.stringify(run)}`)
}

async function pushToDB() {}
