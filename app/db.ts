import cli from "./cli.js";
import mariadb from "mariadb";
import { Run, StatusCodes } from "energy-label-types";
import { exit } from "process";

enum DBTYPES {
	Int,
	Text,
	ForeignKey,
	PrimaryKey,
}
enum Tables {
	PluginName = "PluginName",
	Plugin = "Plugin",
	BrowserName = "BrowserName",
	Browser = "Browser",
	Domain = "Domain",
	ErrorMessage = "ErrorMessage",
	Url = "Url",
	Fact = "Fact",
}

type SchemaFK<T> = {
	dbtype: DBTYPES.ForeignKey;
	name: string;
	table: Tables;
	child: Schema<T>;
	optional?: keyof T;
};

type Schema<T> = Array<
	| { dbtype: DBTYPES.Int; name: string; runkey: keyof T }
	| { dbtype: DBTYPES.Text; name: string; runkey: keyof T }
	| SchemaFK<T>
	| { dbtype: DBTYPES.PrimaryKey; name: string }
>;

/**
 * Sanitize text input to avoid SQL injections
 * */
function sanitize(s: string) {
	return s.replace("'", "\\'");
}

/**
 * A Rrepresentation of the tabels in the database
 * */
const schema: Schema<Run> = [
	{ dbtype: DBTYPES.Int, name: "score", runkey: "score" },
	{ dbtype: DBTYPES.Int, name: "status_code", runkey: "statusCode" },
	{
		dbtype: DBTYPES.ForeignKey,
		name: "error_message",
		table: Tables.ErrorMessage,
		optional: "errorMessage",
		child: [
			{ dbtype: DBTYPES.PrimaryKey, name: "id" },
			{
				dbtype: DBTYPES.Text,
				name: "error_message",
				runkey: "errorMessage",
			},
		],
	},
	{
		dbtype: DBTYPES.ForeignKey,
		name: "plugin_id",
		table: Tables.Plugin,
		child: [
			{ dbtype: DBTYPES.PrimaryKey, name: "id" },
			{ dbtype: DBTYPES.Text, name: "version", runkey: "pluginVersion" },
			{
				dbtype: DBTYPES.Text,
				name: "extention_version",
				runkey: "extensionVersion",
			},
			{
				dbtype: DBTYPES.ForeignKey,
				name: "plugin_name_id",
				table: Tables.PluginName,
				child: [
					{ dbtype: DBTYPES.PrimaryKey, name: "id" },
					{
						dbtype: DBTYPES.Text,
						name: "name",
						runkey: "pluginName",
					},
				],
			},
		],
	},
	{
		dbtype: DBTYPES.ForeignKey,
		name: "browser_id",
		table: Tables.Browser,
		child: [
			{ dbtype: DBTYPES.PrimaryKey, name: "id" },
			{ dbtype: DBTYPES.Text, name: "version", runkey: "browserVersion" },
			{
				dbtype: DBTYPES.ForeignKey,
				name: "browser_name_id",
				table: Tables.BrowserName,
				child: [
					{ dbtype: DBTYPES.PrimaryKey, name: "id" },
					{
						dbtype: DBTYPES.Text,
						name: "browser_name",
						runkey: "browserName",
					},
				],
			},
		],
	},
	{
		dbtype: DBTYPES.ForeignKey,
		name: "url_id",
		table: Tables.Url,
		child: [
			{ dbtype: DBTYPES.PrimaryKey, name: "id" },
			{ dbtype: DBTYPES.Text, name: "path", runkey: "path" },
			{
				dbtype: DBTYPES.ForeignKey,
				name: "domain_id",
				table: Tables.Domain,
				child: [
					{ dbtype: DBTYPES.PrimaryKey, name: "id" },
					{ dbtype: DBTYPES.Text, name: "domain", runkey: "url" },
				],
			},
		],
	},
];

const dummyParent: SchemaFK<Run> = {
	dbtype: DBTYPES.ForeignKey,
	name: "fact",
	table: Tables.Fact,
	child: schema,
};

export default class DB {
	private pool: mariadb.Pool;
	constructor() {
		this.pool = this.connect();
	}
	/**
	 * Create a connection pool
	 * */
	private connect() {
		const user = cli.fallback("energylabel").get("--mariadb-user");
		const password = cli.fallback("energylabel").get("--mariadb-password");
		const database = cli.fallback("energylabel").get("--mariadb-database");
		const host = cli.fallback("localhost").get("--mariadb-host");
		const port: number = Number(cli.fallback("3306").get("--mariadb-port"));
		const connLimit: number = Number(cli.fallback("3306").get("--mariadb-conn-limit"));

		if (isNaN(port)) throw Error("mariadb-port must be a number");
		if (isNaN(connLimit))
			throw Error("mariadb-conn-limit must be a number");

		return mariadb.createPool({
			user: user,
			host: host,
			port: port,
			password: password,
			database: database,
			connectionLimit: connLimit,
		});
	}

	/**
	 * Pipeline a list of commands
	 * */
	private async query(queries: string[]) {
		let conn: mariadb.Connection | null = null;
		try {
			conn = await this.pool.getConnection();
			conn.beginTransaction();
			for (const query of queries) {
				await conn.query(query);
			}
			await conn.commit();
		} catch (e) {
			console.error(e);
			if (conn !== null) await conn.rollback();
		} finally {
			if (conn !== null) await conn.end();
		}
	}

	/**
	 * Initialize tables in database
	 * */
	async init() {
		const queries = compileTables(schema, dummyParent, {});
		await this.query(queries);
	}

	/**
	 * Insert runs into the database
	 * */
	async insertRuns(...runs: Run[]) {
		const query = runs.flatMap((run) =>
			insertRun(schema, dummyParent, run),
		);
		await this.query(query);
	}

	/**
	 * WARNING UNSAFE!!!
	 * This function WILL delete all data in the database
	 * */
	async dropTables() {
		const query = dropTables(schema, dummyParent, {}).reverse();
		await this.query(query);
	}
}

/**
 * Gets name of id in child schema
 * */
function findid<T>(schema: Schema<T>) {
	for (const field of schema) {
		if (field.dbtype === DBTYPES.PrimaryKey) {
			return field.name;
		}
	}
	console.error("A foreing key must be able to reference a primary key");
	exit(1);
}

/**
 * Returns [keys, values] of a schema based on the values of in a Run
 * */
function keysAndValues(
	schema: Schema<Run>,
	run: Run,
): [Array<string>, Array<string>] {
	const keys = schema
		.map((field) => {
			switch (field.dbtype) {
				case DBTYPES.PrimaryKey:
					return "";
				default:
					return field.name;
			}
		})
		.filter((s) => s !== "");

	const values = schema
		.map((field) => {
			switch (field.dbtype) {
				case DBTYPES.PrimaryKey:
					return "";
				case DBTYPES.Int:
					return (
						"'" +
						sanitize(
							((run[field.runkey] as number) ?? 0).toString(),
						) +
						"'"
					);
				case DBTYPES.Text:
					return (
						"'" +
						sanitize((run[field.runkey] as string) ?? "NULL") +
						"'"
					);
				case DBTYPES.ForeignKey:
					return `(SELECT id FROM ${field.table} WHERE ${createWhere(field.child, run)} LIMIT 1)`;
			}
		})
		.filter((s) => s !== "");

	return [keys, values];
}

/**
 * Run a recursive command on all tables in the schema
 * fact: Command special to the facttable
 * child: Command special to all children
 * */
function traverseSchema<S, V extends object>(
	fact: (schema: Schema<S>, parent: SchemaFK<S>, val: V) => string[],
	child: (schema: Schema<S>, parent: SchemaFK<S>, val: V) => string[],
	allwaysExtend: boolean,
) {
	return (schema: Schema<S>, parent: SchemaFK<S>, val: V) => {
		let stmt: Array<string> = [];
		// TRAVERSE CHILDREN
		for (const field of schema) {
			if (
				field.dbtype === DBTYPES.ForeignKey &&
				(allwaysExtend ||
					field.optional === undefined ||
					field.optional in val)
			) {
				stmt = [
					...stmt,
					...traverseSchema(child, child, allwaysExtend)(
						field.child,
						field,
						val,
					),
				];
			}
		}

		// TRAVERSE SELF
		return [...stmt, ...fact(schema, parent, val)];
	};
}

/**
 * Create the where clause of a child schema
 * */
function createWhere<T>(schema: Schema<T>, run: T): string {
	return schema
		.map((field) => {
			switch (field.dbtype) {
				case DBTYPES.Int:
					return `${field.name}='${sanitize((run[field.runkey] ?? 0).toString())}'`;
				case DBTYPES.Text:
					return `${field.name}='${sanitize((run[field.runkey] ?? "NULL").toString())}'`;
				case DBTYPES.ForeignKey:
					return `${field.name}=(SELECT id FROM ${field.table} WHERE ${createWhere(field.child, run)} LIMIT 1)`;
				case DBTYPES.PrimaryKey:
					return "";
			}
		})
		.filter((s) => s !== "")
		.join(" AND ");
}

/**
 * Insert a run in to the database
 * */
const insertRun = traverseSchema<Run, Run>(
	(schema, parent, run) => {
		const [keys, values] = keysAndValues(schema, run);
		return [
			`INSERT INTO ${parent.table} (${keys.join(",")}) VALUES (${values.join(",")})`,
		];
	},
	(schema, parent, run) => {
		let stmt = "";

		const [keys, values] = keysAndValues(schema, run);

		stmt +=
			`INSERT INTO ${parent.table} (${keys.join(",")}) SELECT ${values.join(",")} ` +
			`WHERE NOT EXISTS (SELECT 1 FROM ${parent.table} WHERE ${createWhere(parent.child, run)} LIMIT 1);`;
		return [stmt];
	},
	false,
);

/**
 * Create a  single table
 * */
function createTable<T>(schema: Schema<T>, parent: SchemaFK<T>) {
	let stmt = "";
	stmt += `CREATE TABLE ${parent.table}(`;
	stmt += schema
		.map((field) => {
			switch (field.dbtype) {
				case DBTYPES.Int:
					return `${field.name} INT UNSIGNED`;
				case DBTYPES.Text:
					return `${field.name} TINYTEXT`;
				case DBTYPES.PrimaryKey:
					return `${field.name} INT UNSIGNED PRIMARY KEY AUTO_INCREMENT`;
				case DBTYPES.ForeignKey:
					return `${field.name} INT UNSIGNED REFERENCES ${field.table}(${findid(field.child)})`;
			}
		})
		.join(",");
	stmt += `)`;
	if (cli.fallback("true").get("--mariadb-column-store") === "true") {
		stmt += "ENGINE = ColumnStore;";
	} else {
		stmt += ";";
	}

	return [stmt];
}

/**
 * Create queries to create all tables in the schema
 * */
const compileTables = traverseSchema<Run, object>(
	createTable,
	createTable,
	true,
);

/**
 * Create a drop table query
 * */
const createDropTableQuery = <T>(
	...[, parent]: [Schema<T>, SchemaFK<T>, object]
) => {
	return [`DROP TABLE ${parent.table};`];
};

/**
 * Create queries to drop all tables in a schema
 * */
const dropTables = traverseSchema(
	createDropTableQuery,
	createDropTableQuery,
	true,
);

/**
 * Insert a testrun in the database
 * */
export async function insertTestRun() {
	const run: Run = {
		score: 10,
		statusCode: StatusCodes.TestRun,
		errorMessage: "IT'S A TEST :)",
		browserName: "TestBrowser",
		browserVersion: "t1.234",
		pluginVersion: "t1.23.415",
		pluginName: "DBTest",
		path: "/db/test",
		url: "https://testdb.aau.dk",
		extensionVersion: "0.0.1",
	};

	const run2: Run = {
		score: 10,
		statusCode: StatusCodes.TestRun,
		browserName: "TestBrowser",
		browserVersion: "t1.234",
		pluginVersion: "t1.23.415",
		pluginName: "DBTest",
		path: "/db/test",
		url: "https://testdb.aau.dk",
		extensionVersion: "0.0.1",
	};

	await new DB().insertRuns(run, run2);
}
