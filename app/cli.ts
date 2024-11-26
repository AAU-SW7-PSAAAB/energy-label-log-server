import DB, { insertTestRun } from "./db.js";

/**
 * The help message that is written once --help is given as an argument
 */
const help = `
--help         :: Prints this message
--host=<value> :: Sets the host of the server (default = 127.0.0.1)
--port=<number> :: Sets the port of the server (default = 3000)
--mariadb-user=<value>     :: Sets the user of the MariaDB connection (default = energylabel)
--mariadb-password=<value> :: Sets the password of the MariaDB connection (default = energylabel)
--mariadb-database=<value> :: Sets the database of the MariaDB connection (default = energylabel)
--mariadb-port=<number> :: Sets the port of the mariadb connection (default = 3306)
--mariadb-host=<value> :: Sets the port of the mariadb connection (default = localhost)
--mariadb-init :: Initializes the database
--mariadb-column-store=<bool> :: Sets the storage to column store in --mariadb-init (default = true)
--mariadb-unsafe-drop-tables :: UNSAFE: Drops all tables in the database
--mariadb-insert-test-run :: Inserts a testrun in the database
--mariadb-conn-limit=<number> :: Limit the number of connections the server is allowed to make to the MariaDB database (default = 50) 
`;

/**
 * The valid argument keys of the server
 */
const multiArgs = [
	"--host",
	"--port",
	"--mariadb-user",
	"--mariadb-password",
	"--mariadb-database",
	"--mariadb-port",
	"--mariadb-host",
	"--mariadb-column-store",
	"--mariadb-conn-limit",
] as const;

/**
 * The valid single arguments,
 * NOTE IF YOU CHANGE THIS THEN CREATE A CODITION AT THE END OF THIS FILE
 * */
const singleArgs = [
	"--help",
	"--mariadb-init",
	"--mariadb-insert-test-run",
	"--mariadb-unsafe-drop-tables",
];

/**
 * The literal type of valid argument keys of the server
 */
type MultiArgs = (typeof multiArgs)[number];

/**
 * The object type containing cli arguments
 */
type CliArgs = { [key in MultiArgs]: string | null };

/**
 * The class responcible for handeling commandline arguments
 */
export class Cli {
	private args: CliArgs;
	constructor(cliInput: string[]) {
		this.args = multiArgs.reduce(
			(a, v) => ({ ...a, [v]: null }),
			{},
		) as CliArgs;

		for (const arg of cliInput) {
			const [key, value] = arg.split("=");

			if (singleArgs.includes(key)) {
				continue;
			}

			if (!multiArgs.includes(key as MultiArgs)) {
				console.error(
					`${key} is not a valid key, ensure the argument is of the form --key=value.` +
						`Use --help to get a list of valid arguments`,
				);
				process.exit(-1);
			}

			if (value === undefined) {
				console.error(
					`Failed to parse arg ${key} ensure it is of the form --key=value`,
				);
				process.exit(1);
			}

			this.args[key as MultiArgs] = value;
		}
	}

	/**
	 * Set the default value before getting an arguemt
	 */
	fallback(value: string) {
		return new Fallback(this.args, value);
	}
}

/**
 * Set the default value if the argument is not set
 */
class Fallback {
	private args: CliArgs;
	private default: string;
	constructor(args: CliArgs, def: string) {
		this.args = args;
		this.default = def;
	}

	/**
	 * Get a commandline argument, if it is not set return the fallback value
	 */
	get(...fields: MultiArgs[]) {
		for (const argument of fields) {
			const value = this.args[argument];
			if (value !== null) {
				return value;
			}
		}

		return this.default;
	}
}

// process.argv.slice(2) because processs.argv is
// /path/to/node /path/to/mainfile.js ...args
export default new Cli(process.argv.slice(2));

/**
 * Check all single args the in the cli.
 * */
export async function checkSingleArgs() {
	for (const arg of process.argv) {
		if (arg === "--help") {
			console.log(help);
			process.exit(0);
		}

		if (arg === "--mariadb-init") {
			await new DB().init();
			process.exit(0);
		}

		if (arg === "--mariadb-insert-test-run") {
			await insertTestRun();
			process.exit(0);
		}

		if (arg === "--mariadb-unsafe-drop-tables") {
			await new DB().dropTables();
			process.exit(0);
		}
	}
}
