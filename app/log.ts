import { Run, run as zrun } from "energy-label-types";
import type DB from "./db.js";

/**
 * Log a run or list of runs in the database
 * */
export async function log(db: DB, run: Run | Run[]) {
	if (!Array.isArray(run)) {
		run = [run];
	}
	const parsedruns = run.map(r => zrun.parse(r));
	await db.insertRuns(...parsedruns);
}
