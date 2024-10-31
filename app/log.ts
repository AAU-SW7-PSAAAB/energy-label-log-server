import { Run, run as zrun} from "energy-label-types";
import cli from "./cli.js";
import mariadb from "mariadb";
import DB from "./db.js";

export async function log(run: Run | Run[]) {
	if(!Array.isArray(run)){
		run = [run]	
	}
	const parsedruns = run.map(r => zrun.parse(r));
	await new DB().insertRuns(...parsedruns)
}

