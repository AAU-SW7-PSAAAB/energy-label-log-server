import { Run } from 'energy-label-types'
import cli from './cli.js'
import mariadb from "mariadb"

export async function log(run: Run) {
    console.log(`Loggin run ${JSON.stringify(run)}`)
}

async function pushToDB() {
    const user = cli.default("energylabel").get("--mariadb-user");
    const password = cli.default("energylabel").get("--mariadb-password");
    const database = cli.default("energylabel").get("--mariadb-database");
    const host = cli.default("localhost").get("--mariadb-host");
    const port = Number(cli.default("3306").get("--mariadb-port"));
    
    const pool = mariadb.createPool({
        user : user,
        host: host,
        port: port,
        password : password,
        database : database,
        connectionLimit : 5
    });



}


