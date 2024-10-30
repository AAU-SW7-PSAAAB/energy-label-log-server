import cli from "./cli.js";
import mariadb from "mariadb";
import { Run } from "energy-label-types";

class DB {
	constructor() {}
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

	init() {
		const pool = this.connect();
		const query = `${plugin}${browser}${url}${fact}`.replace(/\s+/g, " ");
		pool.batch(query);
	}

	insertRuns(...runs: Run[]) {
		const pool = this.connect();

		const query =
			runs.reduce(
				(prev: string, run) => prev + insertRun(run),
				"START TRANSACTION",
			) + "COMMIT;";

		pool.batch(query);
	}
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

function insertRun(run: Run) {
	const procs = (
		[
			[
				Tables.PluginName,
				[["name", run.plutingName.replace("'", "\\'")]],
			],
		] as Array<[Tables, Array<[string, string]>]>
	)
		.reduce(
			(prev: string, [tb, pairs]) =>
				prev +
				`DELIMITER $$
                    CREATE FUNCTION #insertif${tb} RETURNS INT 
                        DETERMINISTIC
                    BEGIN
                        DECLARE id INT;
                        IF EXISTS(SELECT id FROM '${tb}' WHERE ${pairs.map(([key, val]) => `'${key}' = '${val}'`).join(" AND ")}))
                            BEGIN
                                RETURN (SELECT id FROM '${tb}' WHERE ${pairs.map(([key, val]) => `'${key}' = '${val}'`).join(" AND ")});
                            END
                        ELSE
                            BEGIN
                                INSERT INTO ${tb} (${pairs.map(([key]) => `'${key}'`).join(",")}) 
                                    VALUES (${pairs.map(([, val]) => `'${val}'`).join(",")}));
                                RETURN (SELECT MAX(id) FROM ${tb});
                            END;
                    END
                $$
                DELIMITER ;`,
			"",
		)
		.replace(/\s+/g, " ");
}

const plugin = `
CREATE TABLE ${Tables.PluginName} (
    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    name TINYTEXT,
) ENGINE=ColumnStore;

CREATE TABLE Plugin (
    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    version TINYTEXT,
    extention_version TINYTEXT,
    plugin_id INT UNSIGNED REFERENCES PluginName(id)
) ENGINE=ColumnStore;
`;

const browser = `
CREATE TABLE ${Tables.BrowserName} (
    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    name TINYTEXT PRIAMRY KEY,
) ENGINE=ColumnStore;

CREATE TABLE ${Tables.Browser}(
    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    version TINYTEXT,
    browser_id INT UNSIGNED REFERENCES BrowserName(id)
) ENGINE=ColumnStore;
`;

const url = `
CREATE TABLE ${Tables.Domain}(
    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    domain TINYTEXT,
) ENGINE=ColumnStore;

CREATE TABLE ${Tables.Url}(
    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    path TINYTEXT,
    domain_id INT UNSIGNED REFERENCES Domain(id)
) ENGINE=ColumnStore;
`;

const fact = `
CREATE TABLE ${Tables.Fact}(
    status_code INT UNSIGNED,
    score TINYINT UNSIGNED,
    plugin_id INT UNSIGNED REFERENCES Plugin(id),
    url_id INT UNSIGNED REFERENCES Url(id),
    browser_id INT UNSIGNED REFERENCES Browser(id) 
) ENGINE=ColumnStore;
`;
