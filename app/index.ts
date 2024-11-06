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

export async function main() {
	await checkSingleArgs();

	const port = Number(cli.default("3000").get("--port"));
	const host = cli.default("localhost").get("--host");

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
		handler: (request) => {
			const body = request.body;
			log(body);
		},
	});

	try {
		await app.listen({ host: host, port: port });
	} catch (err) {
		app.log.error(err);
		process.exit(1);
	}
}
