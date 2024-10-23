/**
 * The help message that is written once --help is given as an argument
 */
const help = `
--help         :: Prints this message
--host=<value> :: Sets the host of the server (default = 127.0.0.1)
--port=<value> :: Sets the host of the server (default = 3000)
`

/**
 * The valid argument keys of the server
 */
const validArgs = ['--host', '--port'] as const

/**
 * The literal type of valid argument keys of the server
 */
type ValidArgs = (typeof validArgs)[number]

/**
 * The type of arguments object used to
 */
type CliArgs = { [key in ValidArgs]: string | null }

/**
 * The class responcible for handeling commandline arguments
 */
export class Cli {
    private args: CliArgs
    constructor(args: string[]) {
        this.args = validArgs.reduce(
            (a, v) => ({ ...a, [v]: null }),
            {}
        ) as CliArgs

        console.log(this.args)

        for (const arg of args) {
            if (arg === '--help') {
                console.log(help)
                process.exit(0)
            }

            const [key, value] = arg.split('=')

            if (!validArgs.includes(key as ValidArgs)) {
                console.error(
                    `${key} is not a valid key, enshure the argument is on the form --key=value.` +
                        `Use --help to get a list of valid arguments`
                )
                process.exit(-1)
            }

            if (value === undefined) {
                console.error(
                    `Failed to parse arg ${key} enshure it is on the form --key=value`
                )
                process.exit(1)
            }

            this.args[key as ValidArgs] = value
        }
    }

    /**
     * Set the default value before getting an arguemt
     */
    default(value: string) {
        return new Default(this.args, value)
    }
}

/**
 * Set the default value if the
 */
class Default {
    private args: CliArgs
    private default: string
    constructor(args: CliArgs, def: string) {
        this.args = args
        this.default = def
    }

    /**
     * Get a commandline argument, if it is not set return the default value
     */
    get(...fields: ValidArgs[]) {
        console.log('Args', this.args)
        for (const argument of fields) {
            const value = this.args[argument]
            if (value !== null) {
                return value
            }
        }

        return this.default
    }
}

export default new Cli(process.argv.slice(2))
