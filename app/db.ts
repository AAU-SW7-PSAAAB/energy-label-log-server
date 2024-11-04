import cli from "./cli.js";
import mariadb from "mariadb";
import { Run, StatusCodes } from "energy-label-types";
import { exit } from "process";

enum DBTYPES {
	INT,
	TEXT,
	FK,
	PK,
}
enum Tables {
	PluginName = "PluginName",
	Plugin = "Plugin",
	BrowserName = "BrowserName",
	Browser = "Browser",
	Domain = "Domain",
	Url = "Url",
	Fact = "Fact",
}

type SchemaFK = { ty: DBTYPES.FK; name: string; table: Tables; child: Schema };

type Schema = Array<
	| { ty: DBTYPES.INT; name: string; runkey: keyof Run }
	| { ty: DBTYPES.TEXT; name: string; runkey: keyof Run }
	| SchemaFK
	| { ty: DBTYPES.PK; name: string }
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
const schema: Schema = [
	{ ty: DBTYPES.INT, name: "score", runkey: "score" },
	{ ty: DBTYPES.INT, name: "status_code", runkey: "statusCode" },
	{
		ty: DBTYPES.FK,
		name: "plugin_id",
		table: Tables.Plugin,
		child: [
			{ ty: DBTYPES.PK, name: "id" },
			{ ty: DBTYPES.TEXT, name: "version", runkey: "pluginVersion" },
			{
				ty: DBTYPES.TEXT,
				name: "extention_version",
				runkey: "extensionVersion",
			},
			{
				ty: DBTYPES.FK,
				name: "plugin_name_id",
				table: Tables.PluginName,
				child: [
					{ ty: DBTYPES.PK, name: "id" },
					{ ty: DBTYPES.TEXT, name: "name", runkey: "pluginName" },
				],
			},
		],
	},
	{
		ty: DBTYPES.FK,
		name: "browser_id",
		table: Tables.Browser,
		child: [
			{ ty: DBTYPES.PK, name: "id" },
			{ ty: DBTYPES.TEXT, name: "version", runkey: "browserVersion" },
			{
				ty: DBTYPES.FK,
				name: "browser_version",
				table: Tables.BrowserName,
				child: [
					{ ty: DBTYPES.PK, name: "id" },
					{
						ty: DBTYPES.TEXT,
						name: "browser_name",
						runkey: "browserName",
					},
				],
			},
		],
	},
	{
		ty: DBTYPES.FK,
		name: "url_id",
		table: Tables.Url,
		child: [
			{ ty: DBTYPES.PK, name: "id" },
			{ ty: DBTYPES.TEXT, name: "path", runkey: "path" },
			{
				ty: DBTYPES.FK,
				name: "domain_id",
				table: Tables.Domain,
				child: [
					{ ty: DBTYPES.PK, name: "id" },
					{ ty: DBTYPES.TEXT, name: "domain", runkey: "url" },
				],
			},
		],
	},
];

const dummyParent: SchemaFK = {
	ty: DBTYPES.FK,
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
		const user = cli.default("energylabel").get("--mariadb-user");
		const password = cli.default("energylabel").get("--mariadb-password");
		const database = cli.default("energylabel").get("--mariadb-database");
		const host = cli.default("localhost").get("--mariadb-host");
		const port = Number(cli.default("3306").get("--mariadb-port"));

		return mariadb.createPool({
			user: user,
			host: host,
			port: port,
			password: password,
			database: database,
			connectionLimit: 5,
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
		const queries = compileTables(schema, dummyParent, undefined);
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
		const query = dropTables(schema, dummyParent, undefined).reverse();
		await this.query(query);
	}
}

/**
 * Gets name of id in child schema
 * */
function findid(schema: Schema) {
	for (const field of schema) {
		if (field.ty === DBTYPES.PK) {
			return field.name;
		}
	}
	console.error("A foreing key must be able to reference a primary key");
	exit(1);
}

/**
 * Returns [keys, values] of a schema
 * */
function keysAndValues(
	schema: Schema,
	run: Run,
): [Array<string>, Array<string>] {
	const keys = schema
		.map((field) => {
			switch (field.ty) {
				case DBTYPES.PK:
					return "";
				default:
					return field.name;
			}
		})
		.filter((s) => s !== "");

	const values = schema
		.map((field) => {
			switch (field.ty) {
				case DBTYPES.PK:
					return "";
				case DBTYPES.TEXT:
				case DBTYPES.INT:
					return "'" + sanitize(run[field.runkey].toString()) + "'";
				case DBTYPES.FK:
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
function traverseSchema<T>(
	fact: (schema: Schema, parent: SchemaFK, val: T) => string[],
	child: (schema: Schema, parent: SchemaFK, val: T) => string[],
) {
	return (schema: Schema, parent: SchemaFK, val: T) => {
		let stmt: Array<string> = [];
		// TRAVERSE CHILDREN
		for (const field of schema) {
			if (field.ty === DBTYPES.FK) {
				stmt = [
					...stmt,
					...traverseSchema(child, child)(field.child, field, val),
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
function createWhere(schema: Schema, run: Run): string {
	return schema
		.map((field) => {
			switch (field.ty) {
				case DBTYPES.INT:
					return `${field.name}='${run[field.runkey]}'`;
				case DBTYPES.TEXT:
					return `${field.name}='${sanitize(run[field.runkey] as string)}'`;
				case DBTYPES.FK:
					return `${field.name}=(SELECT id FROM ${field.table} WHERE ${createWhere(field.child, run)} LIMIT 1)`;
				case DBTYPES.PK:
					return "";
			}
		})
		.filter((s) => s !== "")
		.join(" AND ");
}

/**
 * Insert a run in to the database
 * */
const insertRun = traverseSchema<Run>(
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
			`INSERT INTO ${parent.table} (${keys.join(",")}) SELECT ${values.join(",")} 
			WHERE NOT EXISTS (SELECT 1 FROM ${parent.table} WHERE ${createWhere(parent.child, run)} LIMIT 1);`.replace(
				/\s+/g,
				" ",
			);

		return [stmt];
	},
);

/**
 * Create a  single table
 * */
const createTable = (schema: Schema, parent: SchemaFK) => {
	let stmt = "";
	stmt += `CREATE TABLE ${parent.table}(`;
	stmt += schema
		.map((field) => {
			switch (field.ty) {
				case DBTYPES.INT:
					return `${field.name} INT UNSIGNED`;
				case DBTYPES.TEXT:
					return `${field.name} TINYTEXT`;
				case DBTYPES.PK:
					return `${field.name} INT UNSIGNED PRIMARY KEY AUTO_INCREMENT`;
				case DBTYPES.FK:
					return `${field.name} INT UNSIGNED REFERENCES ${field.table}(${findid(field.child)})`;
			}
		})
		.join(",");
	stmt += `)`;
	if (cli.default("true").get("--mariadb-column-store") === "true") {
		stmt += "ENGINE = ColumnStore;";
	} else {
		stmt += ";";
	}

	return [stmt];
};

/**
 * Create queries to create all tables in the schema
 * */
const compileTables = traverseSchema<undefined>(createTable, createTable);

/**
 * Drop a table
 * */
const createDropTables = (...[, parent]: [Schema, SchemaFK, undefined]) => {
	return [`DROP TABLE ${parent.table};`];
};

/**
 * Create queries to drop all tables in a schema
 * */
const dropTables = traverseSchema(createDropTables, createDropTables);

/**
 * Insert a testrun in the database
 * */
export async function insertTestRun() {
	const run: Run = {
		score: 10,
		statusCode: 10000 as StatusCodes,
		browserName: "TestBrowser",
		browserVersion: "t1.234",
		pluginVersion: "t1.23.415",
		pluginName: "DBTest",
		path: "/db/test",
		url: "https://testdb.aau.dk",
		extensionVersion: "0.0.1",
	};

	await new DB().insertRuns(run);
}
