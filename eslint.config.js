import globals from "globals";
import pluginJs from "@eslint/js";
import tseslint from "typescript-eslint";

export default [
	{
		ignores: ["dist", ".github"],
	},
	{ files: ["**/*.{js,mjs,cjs,ts}"] },
	{ languageOptions: { globals: globals.node } },
	pluginJs.configs.recommended,
	{
		rules: {
			"@typescript-eslint/no-explicit-any": "off",
		},
	},
	...tseslint.configs.recommended,
];
