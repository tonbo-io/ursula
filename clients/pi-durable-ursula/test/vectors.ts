// Golden vectors produced by the executable model (scratchpad final/model/examples.ts), design §4.2 and §4.6.
export const RECORDS = {
	"ADMISSION": "{\"o\":17,\"ops\":[[\"p\",\"IAAAAAAAAAAB_________4A\",{\"seq\":103,\"entry\":{\"id\":127,\"conversationId\":1,\"kind\":\"pi.user\",\"model\":[{\"role\":\"user\",\"content\":\"hi\"}]}}],[\"p\",\"AgAAAAAAAAB_\",{\"t\":\"e\",\"c\":1}],[\"p\",\"QAAAAAAAAACA\",{\"id\":128,\"conversationId\":1,\"requestId\":\"req-1\",\"type\":\"input\",\"status\":\"placed\",\"entry\":127}],[\"p\",\"QQIAAAAAAAAAgA\",{\"id\":128,\"conversationId\":1,\"requestId\":\"req-1\",\"type\":\"input\",\"status\":\"placed\",\"entry\":127}],[\"p\",\"AgAAAAAAAACA\",{\"t\":\"s\"}],[\"p\",\"QwAAAAAAAAABAAAAAAAAAIA\",null],[\"p\",\"QgAAAAAAAAABcmVxLTEA\",{\"id\":128}],[\"p\",\"MAAAAAAAAACB\",{\"id\":129,\"conversationId\":1,\"kind\":\"pi.generation\",\"version\":1,\"input\":{},\"background\":false,\"abortRequested\":false,\"state\":{\"status\":\"pending\",\"checkpoint\":{}}}],[\"p\",\"MQEAAAAAAAAAgQ\",{\"id\":129,\"conversationId\":1,\"kind\":\"pi.generation\",\"version\":1,\"input\":{},\"background\":false,\"abortRequested\":false,\"state\":{\"status\":\"pending\",\"checkpoint\":{}}}],[\"p\",\"AgAAAAAAAACB\",{\"t\":\"t\"}],[\"p\",\"MgAAAAAAAAABAAAAAAAAAIE\",null],[\"p\",\"M3BpLmdlbmVyYXRpb24AAAAAAAAAAIE\",null],[\"p\",\"UwAAAAAAAAAE_________5g\",{\"version\":1,\"value\":{\"generation\":null}}],[\"x\",\"UwAAAAAAAAAE_________5k\",\"UwAAAAAAAAAF\"],[\"x\",\"VAAAAAAAAAAEAAAAAAAAAAA\",\"VAAAAAAAAAAEAAAAAAAAAGc\"],[\"p\",\"AW5leHRfaWQA\",130]]}",
	"PARTIAL": "{\"o\":17,\"ops\":[[\"p\",\"VAAAAAAAAAAEAAAAAAAAAGg\",{\"version\":1,\"ops\":[[\"a\",[\"live\",\"generation\",\"message\",\"text\"],\"hello wor\"]]}]]}",
	"GENESIS": "{\"o\":0,\"ops\":[[\"p\",\"AWZvcm1hdAA\",{\"pi_durable_keyed\":1,\"tuple\":1}],[\"p\",\"AW93bmVyAA\",{\"epoch\":0,\"nonce\":\"9f2c4e1a7b3d5f60a1b2c3d4e5f60718\",\"host\":\"worker-7\",\"pid\":4711,\"opened_at_ms\":1790000000000,\"mode\":\"fence\"}]]}",
	"CLAIM": "{\"o\":42,\"ops\":[[\"p\",\"AW93bmVyAA\",{\"epoch\":42,\"nonce\":\"c0ffee00112233445566778899aabbcc\",\"host\":\"worker-9\",\"pid\":812,\"opened_at_ms\":1790000360000,\"mode\":\"fail-if-active\"}]]}",
	"CLOSE": "{\"o\":42,\"ops\":[[\"p\",\"AW93bmVyAA\",{\"epoch\":42,\"nonce\":\"c0ffee00112233445566778899aabbcc\",\"host\":\"worker-9\",\"pid\":812,\"opened_at_ms\":1790000360000,\"mode\":\"fail-if-active\",\"closed_at_ms\":1790000720000}]]}"
} as const;

export const KEYS: readonly (readonly [string, string])[] = [
	[
		"e/1/~127",
		"IAAAAAAAAAAB_________4A"
	],
	[
		"x/127",
		"AgAAAAAAAAB_"
	],
	[
		"s/128",
		"QAAAAAAAAACA"
	],
	[
		"s.s/placed/128",
		"QQIAAAAAAAAAgA"
	],
	[
		"x/128",
		"AgAAAAAAAACA"
	],
	[
		"s.c/1/128",
		"QwAAAAAAAAABAAAAAAAAAIA"
	],
	[
		"s.r/1/\"req-1\"",
		"QgAAAAAAAAABcmVxLTEA"
	],
	[
		"t/129",
		"MAAAAAAAAACB"
	],
	[
		"t.s/pending/129",
		"MQEAAAAAAAAAgQ"
	],
	[
		"x/129",
		"AgAAAAAAAACB"
	],
	[
		"t.c/1/129",
		"MgAAAAAAAAABAAAAAAAAAIE"
	],
	[
		"t.k/\"pi.generation\"/129",
		"M3BpLmdlbmVyYXRpb24AAAAAAAAAAIE"
	],
	[
		"d.b/4/~103",
		"UwAAAAAAAAAE_________5g"
	],
	[
		"d.b/4/~102",
		"UwAAAAAAAAAE_________5k"
	],
	[
		"strinc(d.b/4/)",
		"UwAAAAAAAAAF"
	],
	[
		"d.r/4/0",
		"VAAAAAAAAAAEAAAAAAAAAAA"
	],
	[
		"d.r/4/103",
		"VAAAAAAAAAAEAAAAAAAAAGc"
	],
	[
		"d.r/4/104",
		"VAAAAAAAAAAEAAAAAAAAAGg"
	],
	[
		"m/next_id",
		"AW5leHRfaWQA"
	],
	[
		"m/format",
		"AWZvcm1hdAA"
	],
	[
		"m/owner",
		"AW93bmVyAA"
	],
	[
		"t.k/\"\\ud800\"/5",
		"M-2ggAAAAAAAAAAABQ"
	],
	[
		"t.k/\"\\ud801\"/5",
		"M-2ggQAAAAAAAAAABQ"
	],
	[
		"s.r/1/<2000 x \"R\">",
		"QgAAAAAAAAAB_qlzUi6syczOrf_AEPEPP8d5ztBpK79Sfr0W73LnIc4y"
	],
	[
		"d.a/session/0/\"pi.agent\"/single/~2/3",
		"UQEAAAAAAAAAAHBpLmFnZW50AAAA__________0AAAAAAAAAAw"
	]
];
