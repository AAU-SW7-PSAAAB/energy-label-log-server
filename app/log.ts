import { Run, run as zrun } from "energy-label-types";
import DB from "./db.js";

/**
 * Log a run or list of runs in the database
 * */
export async function log(run: Run | Run[]) {
	if (!Array.isArray(run)) {
		run = [run];
	}
	const parsedruns = run.map((r) => zrun.parse(r));
	await new DB().insertRuns(...parsedruns);
}
