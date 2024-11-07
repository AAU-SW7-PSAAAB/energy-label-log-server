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
	Url = "Url",
	Fact = "Fact",
}

type SchemaFK = {
	dbtype: DBTYPES.ForeignKey;
	name: string;
	table: Tables;
	child: Schema;
};

type Schema = Array<
	| { dbtype: DBTYPES.Int; name: string; runkey: keyof Run }
	| { dbtype: DBTYPES.Text; name: string; runkey: keyof Run }
	| SchemaFK
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
const schema: Schema = [
	{ dbtype: DBTYPES.Int, name: "score", runkey: "score" },
	{ dbtype: DBTYPES.Int, name: "status_code", runkey: "statusCode" },
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

const dummyParent: SchemaFK = {
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

		if (isNaN(port)) throw Error("Mariadb port must be a number");

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
	schema: Schema,
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
				case DBTYPES.Text:
				case DBTYPES.Int:
					return "'" + sanitize(run[field.runkey].toString()) + "'";
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
function traverseSchema<T>(
	fact: (schema: Schema, parent: SchemaFK, val: T) => string[],
	child: (schema: Schema, parent: SchemaFK, val: T) => string[],
) {
	return (schema: Schema, parent: SchemaFK, val: T) => {
		let stmt: Array<string> = [];
		// TRAVERSE CHILDREN
		for (const field of schema) {
			if (field.dbtype === DBTYPES.ForeignKey) {
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
			switch (field.dbtype) {
				case DBTYPES.Int:
					return `${field.name}='${run[field.runkey]}'`;
				case DBTYPES.Text:
					return `${field.name}='${sanitize(run[field.runkey] as string)}'`;
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
			`INSERT INTO ${parent.table} (${keys.join(",")}) SELECT ${values.join(",")}` +
			`WHERE NOT EXISTS (SELECT 1 FROM ${parent.table} WHERE ${createWhere(parent.child, run)} LIMIT 1);`;
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
};

/**
 * Create queries to create all tables in the schema
 * */
const compileTables = traverseSchema<undefined>(createTable, createTable);

/**
 * Create a drop table query
 * */
const createDropTableQuery = (...[, parent]: [Schema, SchemaFK, undefined]) => {
	return [`DROP TABLE ${parent.table};`];
};

/**
 * Create queries to drop all tables in a schema
 * */
const dropTables = traverseSchema(createDropTableQuery, createDropTableQuery);

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
