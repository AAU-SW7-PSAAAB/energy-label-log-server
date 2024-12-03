import cli from "./cli.js";
import mariadb from "mariadb";
import { Run, StatusCodes } from "energy-label-types";

enum DBTYPES {
	Int,
	Text,
	ForeignKey,
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

type Results<T> = { [x in Tables]?: T };
type SurogateKeys = { [x in Exclude<Tables, Tables.Fact>]: number };

class SurogateKeyBank {
	cache: Record<string, number> = {};
	private keys: SurogateKeys = {
		[Tables.Url]: 0,
		[Tables.Domain]: 0,
		[Tables.Browser]: 0,
		[Tables.BrowserName]: 0,
		[Tables.Plugin]: 0,
		[Tables.PluginName]: 0,
		[Tables.ErrorMessage]: 0,
	};

	constructor() {}

	set<K extends keyof SurogateKeys>(key: K, value: SurogateKeys[K]) {
		this.keys[key] = value;
	}

	requestKey<K extends keyof SurogateKeys>(
		key: K,
		value: string,
	): { value: SurogateKeys[K]; hit: boolean } {
		const cacheKey = key + value;
		if (cacheKey in this.cache)
			return { value: this.cache[cacheKey], hit: true };

		const newKey = ++this.keys[key];
		this.cache[cacheKey] = newKey;

		setTimeout(() => {
			delete this.cache[cacheKey];
		}, 100000);

		return { value: newKey, hit: false };
	}
}

type SchemaFK<T> = {
	dbtype: DBTYPES.ForeignKey;
	name: string;
	table: Tables;
	child_key: string;
	child: Schema<T>;
	optional?: keyof T;
};

type Schema<T> = Array<
	| { dbtype: DBTYPES.Int; name: string; runkey: keyof T }
	| { dbtype: DBTYPES.Text; name: string; runkey: keyof T }
	| SchemaFK<T>
>;

/**
 * Sanitize text input to avoid SQL injections
 * */
function sanitize(s: string) {
	return s.replace("'", "\\'");
}

/**
 * Sanitize the input or return "NULL" if it is undefined or null
 * */
function valueOrNull(value: any): string {
	if (value === undefined || value === null) return "NULL";
	else return `'${sanitize(String(value))}'`;
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
		child_key: "id",
		child: [
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
		child_key: "id",
		child: [
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
				child_key: "id",
				child: [
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
		child_key: "id",
		child: [
			{ dbtype: DBTYPES.Text, name: "version", runkey: "browserVersion" },
			{
				dbtype: DBTYPES.ForeignKey,
				name: "browser_name_id",
				table: Tables.BrowserName,
				child_key: "id",
				child: [
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
		child_key: "id",
		child: [
			{ dbtype: DBTYPES.Text, name: "path", runkey: "path" },
			{
				dbtype: DBTYPES.ForeignKey,
				name: "domain_id",
				table: Tables.Domain,
				child_key: "id",
				child: [
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
	child_key: "id",
	child: schema,
};

export default class DB {
	private pool: mariadb.Pool;
	private keys = new SurogateKeyBank();
	static async new(): Promise<DB> {
		return await new DB().initKeys();
	}

	constructor() {
		this.pool = this.connect();
	}

	/**
	 * Initializes the keytables to match the databse
	 *
	 * */
	private async initKeys(): Promise<DB> {
		const query = initKeys(schema, dummyParent, {}, {});
		const result = await this.query(query, (a) => a[0].id);
		Object.entries(result)
			.map(([k, r]) => [k, r] as [keyof SurogateKeys, number])
			.forEach((a) => this.keys.set(...a));
		return this;
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
		const connLimit: number = Number(
			cli.fallback("50").get("--mariadb-conn-limit"),
		);

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
	private async query<T>(
		queries: TraverseReturn,
		map: (a: any) => T = (a) => a,
	): Promise<Results<T>> {
		let conn: mariadb.PoolConnection | null = null;
		try {
			conn = await this.pool.getConnection();
			conn.beginTransaction();

			const results = (await Promise.all(
				Object.entries(queries).map(([k, q]) =>
					(conn as mariadb.PoolConnection)
						.query(q)
						.then((res) => [k, map(res)]),
				),
			)) as [Tables, any][];

			await conn.commit();

			return results.reduce((p, [k, q]) => {
				p[k] = q;
				return p;
			}, {} as Results<any>);
		} catch (e) {
			console.error(e);
			if (conn !== null) await conn.rollback();
			throw e;
		} finally {
			if (conn !== null) await conn.end();
		}
	}

	/**
	 * Initialize tables in database
	 * */
	async init() {
		const queries = compileTables(schema, dummyParent, {}, {});
		await this.query(queries);
	}

	/**
	 * Insert runs into the database
	 * */
	async insertRuns(...runs: Run[]) {
		const self = this;
		async function $insertRun(run: Run) {
			console.log(`Inserting into DB: ${JSON.stringify(run)}`);
			const keys = await self.query(
				getKeys(schema, dummyParent, run, {}),
				(a) =>
					a.length === 0
						? undefined
						: ((a[0].id as number) ?? undefined),
			);

			await self.query(
				insertRun(schema, dummyParent, run, [keys, self.keys]),
			);
		}

		await Promise.all(runs.map($insertRun));
	}

	/**
	 * WARNING UNSAFE!!!
	 * This function WILL delete all data in the database
	 * */
	async dropTables() {
		const query = dropTables(schema, dummyParent, {}, {});
		await this.query(query);
	}
}

/**
 * Returns [keys, values] of a schema based on the values of in a Run
 * */
function keysAndValues<T>(
	schema: Schema<T>,
	run: T,
	sorugateKeys: Results<number | undefined>,
): [Array<string>, Array<string>] {
	const keys = schema.map((field) => field.name);

	const values = schema.map((field) => {
		switch (field.dbtype) {
			case DBTYPES.Int:
			case DBTYPES.Text:
				return valueOrNull(run[field.runkey]);
			case DBTYPES.ForeignKey:
				return valueOrNull(sorugateKeys[field.table]);
		}
	});

	return [keys, values];
}

type TraverseReturn = { [key in Tables]?: string };

/**
 * Run a recursive command on all tables in the schema
 * fact: Command special to the facttable
 * child: Command special to all children
 * */
function traverseSchema<S, V extends object, W extends object>(
	fact: (
		schema: Schema<S>,
		parent: SchemaFK<S>,
		val: V,
		options: W,
	) => TraverseReturn,
	child: (
		schema: Schema<S>,
		parent: SchemaFK<S>,
		val: V,
		options: W,
	) => TraverseReturn,
	allwaysExtend: boolean = false,
	condition: (
		schema: Schema<S>,
		parent: SchemaFK<S>,
		val: V,
		options: W,
	) => boolean = () => true,
) {
	return (schema: Schema<S>, parent: SchemaFK<S>, val: V, options: W) => {
		let stmt: Array<string> = [];
		// TRAVERSE CHILDREN
		for (const field of schema) {
			if (
				field.dbtype === DBTYPES.ForeignKey &&
				(allwaysExtend ||
					field.optional === undefined ||
					field.optional in val) &&
				condition(field.child, field, val, options)
			) {
				stmt = {
					...stmt,
					...traverseSchema(child, child, allwaysExtend, condition)(
						field.child,
						field,
						val,
						options,
					),
				};
			}
		}

		// TRAVERSE SELF
		return { ...stmt, ...fact(schema, parent, val, options) };
	};
}

/**
 * Insert a run in to the database
 * */
const insertRun = traverseSchema<
	Run,
	Run,
	[Results<number | undefined>, SurogateKeyBank]
>(
	(schema, parent, run, [sorugateKeys]) => {
		let [keys, values] = keysAndValues(schema, run, sorugateKeys);
		return {
			[parent.table]: `INSERT INTO ${parent.table} (${keys.join(",")}) VALUES (${values.join(",")})`,
		};
	},
	(schema, parent, run, [sorugateKeys]) => {
		let stmt = "";

		const [keys, values] = keysAndValues(schema, run, sorugateKeys);

		keys.push(parent.child_key);
		values.push(valueOrNull(sorugateKeys[parent.table]));

		stmt += `INSERT INTO ${parent.table} (${keys.join(",")}) VALUES (${values.join(",")})`;
		return { [parent.table]: stmt };
	},
	false,
	(schema, parent, run, [sorugateKeys, bank]) => {
		if (parent.table === Tables.Fact) return true;
		if (sorugateKeys[parent.table] !== undefined) {
			return false;
		}
		// Increment sorugate key
		const { value, hit } = bank.requestKey(
			parent.table,
			createCacheKey(schema, run),
		);
		sorugateKeys[parent.table] = value;
		return !hit;
	},
);

function createCacheKey<T>(schema: Schema<T>, run: T): string {
	return schema
		.map((a) => {
			switch (a.dbtype) {
				case DBTYPES.ForeignKey:
					return createCacheKey(a.child, run);
				default:
					return run[a.runkey];
			}
		})
		.join("#");
}

/**
 * Create a  single table
 * */
function createTable<T>(hasId: boolean) {
	return (schema: Schema<T>, parent: SchemaFK<T>) => {
		let stmt = `CREATE TABLE ${parent.table}( ${hasId ? `${parent.child_key} INT UNSIGNED,` : ""}`;
		stmt += schema
			.map((field) => {
				switch (field.dbtype) {
					case DBTYPES.Int:
						return `${field.name} INT UNSIGNED`;
					case DBTYPES.Text:
						return `${field.name} TINYTEXT`;
					case DBTYPES.ForeignKey:
						return `${field.name} INT UNSIGNED`;
				}
			})
			.join(",");
		stmt += `)`;
		if (cli.fallback("true").get("--mariadb-column-store") === "true") {
			stmt += "ENGINE = ColumnStore;";
		} else {
			stmt += ";";
		}

		return { [parent.table]: stmt };
	};
}

/**
 * Create queries to create all tables in the schema
 * */
const compileTables = traverseSchema<Run, object, object>(
	createTable(false),
	createTable(true),
	true,
);

/**
 * Create a drop table query
 * */
const createDropTableQuery = <T>(
	...[, parent]: [Schema<T>, SchemaFK<T>, object]
) => {
	return { [parent.table]: `DROP TABLE ${parent.table};` };
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
 * Get the max id of each table
 * */
const initKeys = traverseSchema(
	() => {
		return {};
	},
	<T>(...[, parent]: [Schema<T>, SchemaFK<T>, object]) => {
		return {
			[parent.table]: `SELECT COALESCE(MAX(id), 0) AS id FROM ${parent.table}`,
		};
	},
	true,
);

/**
 * Create an innerjoin of all tables in the schema and children
 * */
function innerJoin<T>(schema: Schema<T>, table: Tables) {
	function $innerJoin(schema: Schema<T>, table: Tables): string[] {
		return schema
			.filter((f) => f.dbtype === DBTYPES.ForeignKey)
			.flatMap((f) => [
				`INNER JOIN ${f.table} ON ${table}.${f.name}=${f.table}.${f.child_key}`,
				...$innerJoin(f.child, f.table),
			]);
	}

	return [table, ...$innerJoin(schema, table)].join(" ");
}

/**
 * Create a where clause of a joined table created by `innerJoin`
 * */
function joinWhere<T extends object>(
	schema: Schema<T>,
	table: Tables,
	value: T,
) {
	function $joinWhere(schema: Schema<T>, table: Tables): string[] {
		return schema.flatMap((f) => {
			switch (f.dbtype) {
				case DBTYPES.Int:
				case DBTYPES.Text:
					return [
						`${table}.${f.name}=${valueOrNull(value[f.runkey])}`,
					];
				case DBTYPES.ForeignKey:
					return $joinWhere(f.child, f.table);
			}
		});
	}

	return $joinWhere(schema, table).join(" AND ");
}


/**
 * The query for `getKeys`
 * */
function getKeysQuery<T extends object>(
	schema: Schema<T>,
	table: Tables,
	id_name: string,
	value: T,
) {
	return `SELECT MAX(${table}.${id_name}) AS id FROM ${innerJoin(schema, table)} WHERE ${joinWhere(schema, table, value)}`;
}

/**
 * Get the key of a specific entry of the database
 * */
const getKeys = traverseSchema(
	() => {
		return {};
	},
	<T extends object>(schema: Schema<T>, parent: SchemaFK<T>, value: T) => {
		return {
			[parent.table]: getKeysQuery(
				schema,
				parent.table,
				parent.child_key,
				value,
			),
		};
	},
);

/**
 * Insert a testrun in the database
 * */
export async function insertTestRun() {
	const run: Run = {
		score: 10,
		statusCode: StatusCodes.TestRun,
		errorMessage: "IT'S A TEST :) x",
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

	await DB.new().then((db) => db.insertRuns(run, run2));
}
