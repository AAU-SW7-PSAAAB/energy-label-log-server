import cli from "./cli.js";
import mariadb from "mariadb";
import { Run, StatusCodes } from "energy-label-types";
import z from "zod";

const zIdResponse = z
	.object({
		id: z
			.string()
			.transform((val, ctx) => {
				const x = Number(val);
				if (!isNaN(x)) return x;
				else {
					ctx.addIssue({
						code: z.ZodIssueCode.custom,
						message: "Not a number",
					});
					return z.NEVER;
				}
			})
			.nullable()
			.or(z.number()),
	})
	.array();

/**
 * Do nothing
 * */
const identity = <T>(a: T) => a;

enum DBTypes {
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
type SurrogateKeys = { [x in Exclude<Tables, Tables.Fact>]: number };

class SurrogateKeyBank {
	cache: Record<string, number> = {};
	private keys: SurrogateKeys = {
		[Tables.Url]: 0,
		[Tables.Domain]: 0,
		[Tables.Browser]: 0,
		[Tables.BrowserName]: 0,
		[Tables.Plugin]: 0,
		[Tables.PluginName]: 0,
		[Tables.ErrorMessage]: 0,
	};

	constructor() {}

	set<K extends keyof SurrogateKeys>(key: K, value: SurrogateKeys[K]) {
		this.keys[key] = value;
	}

	requestKey<K extends keyof SurrogateKeys>(
		key: K,
		value: string,
	): { value: SurrogateKeys[K]; hit: boolean } {
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
	dbtype: DBTypes.ForeignKey;
	name: string;
	table: Tables;
	child_key: string;
	child: Schema<T>;
	optional?: keyof T;
};

type Schema<T> = Array<
	| { dbtype: DBTypes.Int; name: string; runkey: keyof T }
	| { dbtype: DBTypes.Text; name: string; runkey: keyof T }
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
function valueOrNull<T>(value: T): string {
	if (value === undefined || value === null) return "NULL";
	else return `'${sanitize(String(value))}'`;
}

/**
 * A representation of the tabels in the database
 * */
const schema: Schema<Run> = [
	{ dbtype: DBTypes.Int, name: "score", runkey: "score" },
	{ dbtype: DBTypes.Int, name: "status_code", runkey: "statusCode" },
	{
		dbtype: DBTypes.ForeignKey,
		name: "error_message",
		table: Tables.ErrorMessage,
		optional: "errorMessage",
		child_key: "id",
		child: [
			{
				dbtype: DBTypes.Text,
				name: "error_message",
				runkey: "errorMessage",
			},
		],
	},
	{
		dbtype: DBTypes.ForeignKey,
		name: "plugin_id",
		table: Tables.Plugin,
		child_key: "id",
		child: [
			{ dbtype: DBTypes.Text, name: "version", runkey: "pluginVersion" },
			{
				dbtype: DBTypes.Text,
				name: "extention_version",
				runkey: "extensionVersion",
			},
			{
				dbtype: DBTypes.ForeignKey,
				name: "plugin_name_id",
				table: Tables.PluginName,
				child_key: "id",
				child: [
					{
						dbtype: DBTypes.Text,
						name: "name",
						runkey: "pluginName",
					},
				],
			},
		],
	},
	{
		dbtype: DBTypes.ForeignKey,
		name: "browser_id",
		table: Tables.Browser,
		child_key: "id",
		child: [
			{ dbtype: DBTypes.Text, name: "version", runkey: "browserVersion" },
			{
				dbtype: DBTypes.ForeignKey,
				name: "browser_name_id",
				table: Tables.BrowserName,
				child_key: "id",
				child: [
					{
						dbtype: DBTypes.Text,
						name: "browser_name",
						runkey: "browserName",
					},
				],
			},
		],
	},
	{
		dbtype: DBTypes.ForeignKey,
		name: "url_id",
		table: Tables.Url,
		child_key: "id",
		child: [
			{ dbtype: DBTypes.Text, name: "path", runkey: "path" },
			{
				dbtype: DBTypes.ForeignKey,
				name: "domain_id",
				table: Tables.Domain,
				child_key: "id",
				child: [
					{ dbtype: DBTypes.Text, name: "domain", runkey: "url" },
				],
			},
		],
	},
];

const dummyParent: SchemaFK<Run> = {
	dbtype: DBTypes.ForeignKey,
	name: "fact",
	table: Tables.Fact,
	child_key: "id",
	child: schema,
};

export default class DB {
	private pool: mariadb.Pool;
	private keyBank = new SurrogateKeyBank();
	static async new(): Promise<DB> {
		return await new DB().initKeys();
	}

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
	private async query<T, R>({
		queries,
		validator,
		mapResult,
	}: {
		queries: TraverseReturn;
		validator: (data: unknown) => T;
		mapResult: (a: T) => R;
	}): Promise<Results<R>> {
		let conn: mariadb.PoolConnection | null = null;
		try {
			conn = await this.pool.getConnection();
			conn.beginTransaction();

			const results = await Promise.all(
				Object.entries(queries).map(([key, queryString]) =>
					(conn as mariadb.PoolConnection)
						.query(queryString)
						.then(validator)
						.then(mapResult)
						.then((res) => [key, res] as [Tables, R]),
				),
			);

			await conn.commit();

			return results.reduce((p, [key, queryString]) => {
				p[key] = queryString;
				return p;
			}, {} as Results<R>);
		} catch (e) {
			console.error(e);
			if (conn !== null) await conn.rollback();
			throw e;
		} finally {
			if (conn !== null) await conn.end();
		}
	}

	/**
	 * Initializes the keytables to match the databse
	 * */
	private async initKeys(): Promise<DB> {
		const result = await this.query({
			queries: initKeys(schema, dummyParent, {}, {}),
			validator: (r) => {
				const res = zIdResponse.safeParse(r);
				return res.success ? res.data : [{ id: 0 }];
			},
			mapResult: (res) => res[0].id,
		});
		Object.entries(result)
			.map(([key, id]) => [key, id] as [keyof SurrogateKeys, number])
			.forEach((data) => this.keyBank.set(...data));
		return this;
	}

	/**
	 * Insert runs into the database
	 * */
	async insertRuns(...runs: Run[]) {
		await Promise.all(
			runs.map(async (run: Run) => {
				console.log(`Inserting into DB: ${JSON.stringify(run)}`);
				const existingKeys = await this.query({
					queries: getKeys(schema, dummyParent, run, {}),
					validator: zIdResponse.parse,
					mapResult: (res) =>
						res.length === 0
							? undefined
							: ((res[0].id as number) ?? undefined),
				});

				await this.query({
					queries: insertRun(schema, dummyParent, run, [
						existingKeys,
						this.keyBank,
					]),
					validator: identity,
					mapResult: identity,
				});
			}),
		);
	}

	/**
	 * Initialize tables in database
	 * */
	async init() {
		const queries = compileTables(schema, dummyParent, {}, {});
		await this.query({ queries, validator: identity, mapResult: identity });
	}

	/**
	 * WARNING UNSAFE!!!
	 * This function WILL delete all data in the database
	 * */
	async dropTables() {
		const queries = dropTables(schema, dummyParent, {}, {});
		await this.query({ queries, validator: identity, mapResult: identity });
	}
}

/**
 * Returns [keys, values] of a schema based on the values of in a Run
 * */
function keysAndValues<T>(
	schema: Schema<T>,
	run: T,
	surrogateKeys: Results<number | undefined>,
): [Array<string>, Array<string>] {
	const keys = schema.map((field) => field.name);

	const values = schema.map((field) => {
		switch (field.dbtype) {
			case DBTypes.Int:
			case DBTypes.Text:
				return valueOrNull(run[field.runkey]);
			case DBTypes.ForeignKey:
				return valueOrNull(surrogateKeys[field.table]);
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
function traverseSchema<S, V extends object, W extends object>({
	fact,
	dimension,
	allwaysExtend,
	condition,
}: {
	fact: (
		schema: Schema<S>,
		parent: SchemaFK<S>,
		value: V,
		options: W,
	) => TraverseReturn;
	dimension: (
		schema: Schema<S>,
		parent: SchemaFK<S>,
		value: V,
		options: W,
	) => TraverseReturn;
	condition: (
		schema: Schema<S>,
		parent: SchemaFK<S>,
		value: V,
		options: W,
	) => boolean;
	allwaysExtend: boolean;
}) {
	return (schema: Schema<S>, parent: SchemaFK<S>, value: V, options: W) => {
		let stmt: Array<string> = [];
		// TRAVERSE CHILDREN
		for (const field of schema) {
			if (
				field.dbtype === DBTypes.ForeignKey &&
				(allwaysExtend ||
					field.optional === undefined ||
					field.optional in value) &&
				condition(field.child, field, value, options)
			) {
				stmt = {
					...stmt,
					...traverseSchema({
						fact: dimension,
						dimension,
						allwaysExtend,
						condition,
					})(field.child, field, value, options),
				};
			}
		}

		// TRAVERSE SELF
		return { ...stmt, ...fact(schema, parent, value, options) };
	};
}

/**
 * Insert a run into the database
 * */
const insertRun = traverseSchema<
	Run,
	Run,
	[Results<number | undefined>, SurrogateKeyBank]
>({
	fact: (schema, parent, run, [surrogateKeys]) => {
		const [keys, values] = keysAndValues(schema, run, surrogateKeys);
		return {
			[parent.table]: `INSERT INTO ${parent.table} (${keys.join(",")}) VALUES (${values.join(",")})`,
		};
	},
	dimension: (schema, parent, run, [surrogateKeys]) => {
		let stmt = "";

		const [keys, values] = keysAndValues(schema, run, surrogateKeys);

		keys.push(parent.child_key);
		values.push(valueOrNull(surrogateKeys[parent.table]));

		stmt += `INSERT INTO ${parent.table} (${keys.join(",")}) VALUES (${values.join(",")})`;
		return { [parent.table]: stmt };
	},
	condition: (schema, parent, run, [surrogateKeys, bank]) => {
		if (parent.table === Tables.Fact) return true;
		if (surrogateKeys[parent.table] !== undefined) {
			return false;
		}
		// Increment surrogate key
		const { value, hit } = bank.requestKey(
			parent.table,
			createCacheKey(schema, run),
		);
		surrogateKeys[parent.table] = value;
		return !hit;
	},
	allwaysExtend: false,
});

function createCacheKey<T>(schema: Schema<T>, run: T): string {
	return schema
		.map((f) => {
			switch (f.dbtype) {
				case DBTypes.ForeignKey:
					return createCacheKey(f.child, run);
				default:
					return run[f.runkey];
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
					case DBTypes.Int:
						return `${field.name} INT UNSIGNED`;
					case DBTypes.Text:
						return `${field.name} TINYTEXT`;
					case DBTypes.ForeignKey:
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
const compileTables = traverseSchema<Run, object, object>({
	fact: createTable(false),
	dimension: createTable(true),
	condition: () => true,
	allwaysExtend: true,
});

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
const dropTables = traverseSchema({
	fact: createDropTableQuery,
	dimension: createDropTableQuery,
	allwaysExtend: true,
	condition: () => true,
});

/**
 * Get the max id of each table
 * */
const initKeys = traverseSchema({
	fact: () => {
		return {};
	},
	dimension: <T>(...[, parent]: [Schema<T>, SchemaFK<T>, object]) => {
		return {
			[parent.table]: `SELECT COALESCE(MAX(id), 0) AS id FROM ${parent.table}`,
		};
	},
	condition: () => true,
	allwaysExtend: true,
});

/**
 * Create an innerjoin of all tables in the schema and children
 * */
function innerJoin<T>(schema: Schema<T>, table: Tables) {
	function $innerJoin(schema: Schema<T>, table: Tables): string[] {
		return schema
			.filter((f) => f.dbtype === DBTypes.ForeignKey)
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
				case DBTypes.Int:
				case DBTypes.Text:
					return [
						`${table}.${f.name}=${valueOrNull(value[f.runkey])}`,
					];
				case DBTypes.ForeignKey:
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
const getKeys = traverseSchema({
	fact: () => {
		return {};
	},
	dimension: <T extends object>(
		schema: Schema<T>,
		parent: SchemaFK<T>,
		value: T,
	) => {
		return {
			[parent.table]: getKeysQuery(
				schema,
				parent.table,
				parent.child_key,
				value,
			),
		};
	},
	condition: () => true,
	allwaysExtend: false,
});

/**
 * Insert a test run in the database
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
