import Fastify from "fastify";
import cli, { checkSingleArgs } from "./cli.js";
import {
	serializerCompiler,
	validatorCompiler,
	ZodTypeProvider,
} from "fastify-type-provider-zod";
import z from "zod";
import { run } from "energy-label-types";
import { log } from "./log.js";
import DB from "./db.js";
import { SqlError } from "mariadb";

export async function main() {
	await checkSingleArgs();

	const db = await DB.new();

	const port = Number(cli.fallback("3000").get("--port"));
	const host = cli.fallback("localhost").get("--host");

	if (isNaN(port)) {
		throw Error("Port must be a number");
	}

	const app = Fastify({ logger: true });

	app.setValidatorCompiler(validatorCompiler);
	app.setSerializerCompiler(serializerCompiler);

	app.get("/version", async () => ({ version: "0.0.1" }));
	app.withTypeProvider<ZodTypeProvider>().route({
		method: "POST",
		url: "/log",
		schema: {
			body: run,
			response: {
				200: z.string,
			},
		},
		handler: async (request, reply) => {
			const body = request.body;
			try {
				await log(db, body);
				reply.status(200).send();
			} catch (e) {
				if (e instanceof SqlError) reply.status(500).send(e);
				else reply.status(400).send(e);
			}
		},
	});

	try {
		await app.listen({ host: host, port: port });
	} catch (err) {
		app.log.error(err);
		process.exit(1);
	}
}
