import DB, { insertTestRun } from "./db.js";

/**
 * The help message that is written once --help is given as an argument
 */
const help = `
--help         :: Prints this message
--host=<value> :: Sets the host of the server (default = 127.0.0.1)
--port=<value> :: Sets the port of the server (default = 3000)
--mariadb-user=<value>     :: Sets the user of the mariadb connection (default = energylabel)
--mariadb-password=<value> :: Sets the password of the mariadb connection (default = energylabel)
--mariadb-database=<value> :: Sets the database of the mariadb connection (default = energylabel)
--mariadb-port=<value> :: Sets the port of the mariadb connection (default = 3306)
--mariadb-host=<value> :: Sets the port of the mariadb connection (default = localhost)
--mariadb-init :: Initializes the database
--mariadb-column-store=<bool> :: Sets the storage to column store in --mariadb-init (default = true)
`;

/**
 * The valid argument keys of the server
 */
const validArgs = [
	"--host",
	"--port",
	"--mariadb-user",
	"--mariadb-password",
	"--mariadb-database",
	"--mariadb-port",
	"--mariadb-host",
	"--mariadb-column-store",
] as const;

const singleArgs = [
	"--help",
	"--mariadb-init",
	"--mariadb-insert-test-run",
	"--mariadb-unsafe-drop-tables",
];

/**
 * The literal type of valid argument keys of the server
 */
type ValidArgs = (typeof validArgs)[number];

/**
 * The object type containing cli arguments
 */
type CliArgs = { [key in ValidArgs]: string | null };

/**
 * The class responcible for handeling commandline arguments
 */
export class Cli {
	private args: CliArgs;
	constructor(args: string[]) {
		this.args = validArgs.reduce(
			(a, v) => ({ ...a, [v]: null }),
			{},
		) as CliArgs;

		for (const arg of args) {
			const [key, value] = arg.split("=");

			if (singleArgs.includes(key)) {
				continue;
			}

			if (!validArgs.includes(key as ValidArgs)) {
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

			this.args[key as ValidArgs] = value;
		}
	}

	/**
	 * Set the default value before getting an arguemt
	 */
	default(value: string) {
		return new Default(this.args, value);
	}
}

/**
 * Set the default value if the argument is not set
 */
class Default {
	private args: CliArgs;
	private default: string;
	constructor(args: CliArgs, def: string) {
		this.args = args;
		this.default = def;
	}

	/**
	 * Get a commandline argument, if it is not set return the default value
	 */
	get(...fields: ValidArgs[]) {
		for (const argument of fields) {
			const value = this.args[argument];
			if (value !== null) {
				return value;
			}
		}

		return this.default;
	}
}

export default new Cli(process.argv.slice(2));

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
