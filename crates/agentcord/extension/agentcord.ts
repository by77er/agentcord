/**
 * agentcord — pi extension that gives an agent its multi-agent tools.
 *
 * Loaded by the agentcord broker with `pi --mode rpc -e <this file>`. Every tool is a thin
 * request over the broker's Unix socket ($AGENTCORD_SOCKET), made as this agent
 * ($AGENTCORD_AGENT_ID). The broker owns all state and formats the results.
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { StringEnum } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import * as net from "node:net";

const SOCKET = process.env.AGENTCORD_SOCKET;
const ME = process.env.AGENTCORD_AGENT_ID;

interface BrokerResponse {
	ok: boolean;
	error?: string;
	text?: string;
	data?: unknown;
}

function call(req: Record<string, unknown>, signal?: AbortSignal): Promise<BrokerResponse> {
	return new Promise((resolve, reject) => {
		const sock = net.createConnection(SOCKET!);
		let buf = "";
		const onAbort = () => {
			sock.destroy();
			reject(new Error("aborted"));
		};
		signal?.addEventListener("abort", onAbort, { once: true });
		sock.setEncoding("utf8");
		sock.on("connect", () => sock.write(JSON.stringify({ ...req, from: ME }) + "\n"));
		sock.on("data", (chunk: string) => {
			buf += chunk;
			const nl = buf.indexOf("\n");
			if (nl === -1) return;
			signal?.removeEventListener("abort", onAbort);
			sock.end();
			try {
				resolve(JSON.parse(buf.slice(0, nl)));
			} catch (e) {
				reject(e);
			}
		});
		sock.on("error", (e) => {
			signal?.removeEventListener("abort", onAbort);
			reject(new Error(`agentcord broker unreachable: ${e.message}`));
		});
	});
}

export default function (pi: ExtensionAPI) {
	if (!SOCKET || !ME) return;

	const tool = (
		name: string,
		label: string,
		description: string,
		parameters: ReturnType<typeof Type.Object>,
		op: string,
		promptSnippet: string,
	) =>
		pi.registerTool({
			name,
			label,
			description,
			promptSnippet,
			parameters,
			async execute(_toolCallId, params, signal) {
				const res = await call({ op, ...(params as object) }, signal);
				if (!res.ok) throw new Error(res.error ?? "agentcord request failed");
				return {
					content: [{ type: "text", text: res.text ?? JSON.stringify(res.data) }],
					details: res.data ?? {},
				};
			},
		});

	tool(
		"agent_spawn",
		"Spawn agent",
		"Start a new agent working on a task. It runs concurrently; its final response is delivered back to you as a REPORT " +
			"message that wakes you. Returns the new agent's id.",
		Type.Object({
			prompt: Type.String({ description: "Self-contained task for the new agent" }),
			name: Type.Optional(Type.String({ description: "Unique name to claim for it, e.g. 'researcher'" })),
			model: Type.Optional(Type.String({ description: "Model (provider/id); defaults to yours" })),
			thinking: Type.Optional(Type.String({ description: "Thinking level: off|minimal|low|medium|high|xhigh" })),
			cwd: Type.Optional(Type.String({ description: "Working directory, relative to yours" })),
			tools: Type.Optional(Type.String({ description: "Comma-separated allowlist of built-in tools, e.g. 'read,grep,find,ls' (ignored if the session disables built-in tools)" })),
			system: Type.Optional(Type.String({ description: "Extra system prompt text for the agent" })),
			report: Type.Optional(Type.Boolean({ description: "Deliver its final responses to you (default true)" })),
		}),
		"spawn",
		"Spawn a concurrent sub-agent; its final answer comes back to you as a report",
	);

	tool(
		"agent_list",
		"List agents",
		"List all agents in the session with their ids, names, status, parent, subscriptions and task.",
		Type.Object({}),
		"agents",
		"List agents in the session",
	);

	tool(
		"agent_kill",
		"Kill agent",
		"Stop an agent you spawned (directly or indirectly) and all of its descendants.",
		Type.Object({ agent: Type.String({ description: "Agent id or @name" }) }),
		"kill",
		"Stop an agent you spawned",
	);

	tool(
		"claim_name",
		"Claim name",
		"Claim a unique name so others can address you as @name. Replaces any name you held before.",
		Type.Object({ name: Type.String({ description: "a-z, 0-9, '_' or '-', starting with a letter" }) }),
		"claim_name",
		"Claim a unique @name for yourself",
	);

	tool(
		"send_message",
		"Send message",
		"Send a direct message to an agent (by id or @name) or to '@human' (the human operator). It is inserted into the recipient's context and wakes it.",
		Type.Object({
			to: Type.String({ description: "Agent id, @name, or '@human'" }),
			text: Type.String(),
		}),
		"send",
		"Direct-message another agent (wakes it)",
	);

	tool(
		"topic_create",
		"Create topic",
		"Create a topic: a durable, named pub/sub conversation. You are subscribed; invited agents are subscribed too and are " +
			"woken by every post.",
		Type.Object({
			name: Type.Optional(Type.String({ description: "Unique topic name (a-z, 0-9, '_', '-'); addressed as #name" })),
			description: Type.Optional(Type.String()),
			invite: Type.Optional(Type.Array(Type.String(), { description: "Agent ids or @names to subscribe" })),
		}),
		"topic_create",
		"Create a named pub/sub topic, optionally inviting agents",
	);

	tool(
		"topic_name",
		"Name topic",
		"Give a topic a unique name, or rename it (creator only). History is kept; the topic id never changes.",
		Type.Object({
			topic: Type.String({ description: "#name or topic id" }),
			name: Type.String({ description: "New unique name" }),
		}),
		"topic_name",
		"Name or rename a topic",
	);

	tool(
		"topic_invite",
		"Invite to topic",
		"Subscribe other agents to a topic (mode wake), so each post wakes them.",
		Type.Object({
			topic: Type.String({ description: "#name or topic id" }),
			agents: Type.Array(Type.String(), { description: "Agent ids or @names" }),
		}),
		"invite",
		"Subscribe other agents to a topic",
	);

	tool(
		"topic_list",
		"List topics",
		"List every topic with its subscribers and message count, marking the ones you are subscribed to, plus your DM conversations.",
		Type.Object({}),
		"topics",
		"List topics and which ones you are subscribed to",
	);

	tool(
		"subscribe",
		"Subscribe",
		"Subscribe to a topic. mode 'wake' (default): each post wakes you and is inserted into your context. mode 'digest': posts " +
			"wait and ride along with your next wake-up.",
		Type.Object({
			topic: Type.String({ description: "#name or topic id" }),
			mode: Type.Optional(StringEnum(["wake", "digest"] as const)),
		}),
		"subscribe",
		"Subscribe to a topic",
	);

	tool(
		"unsubscribe",
		"Unsubscribe",
		"Stop receiving posts from a topic. You can still read it with read_log; posting to it again re-subscribes you.",
		Type.Object({ topic: Type.String({ description: "#name or topic id" }) }),
		"unsubscribe",
		"Unsubscribe from a topic",
	);

	tool(
		"post",
		"Post",
		"Post a message to a topic; all subscribers receive it. Posting subscribes you (so you hear replies). Posting to an " +
			"unknown #name creates that topic. @name mentions wake the mentioned agents.",
		Type.Object({
			topic: Type.String({ description: "#name or topic id" }),
			text: Type.String(),
		}),
		"post",
		"Post to a pub/sub topic",
	);

	tool(
		"read_log",
		"Read log",
		"Read a window of a topic's or DM conversation's history. With no cursor, returns the latest messages. Use before=<seq> " +
			"to scroll back, after=<seq> to read forward.",
		Type.Object({
			log: Type.String({ description: "#topic, topic id, @agent (your DMs with them) or a dm:... log id" }),
			before: Type.Optional(Type.Integer({ description: "Only messages with seq < before" })),
			after: Type.Optional(Type.Integer({ description: "Only messages with seq > after" })),
			limit: Type.Optional(Type.Integer({ description: "Max messages (default 20, max 200)" })),
		}),
		"read",
		"Scroll back through a topic or DM history",
	);

	pi.registerTool({
		name: "wait_for_messages",
		label: "Wait for messages",
		description:
			"End your turn without producing a report, and sleep until a message for you arrives (a DM, a subscribed topic post, " +
			"a mention, or a report from an agent you spawned). Use it after spawning agents or asking others for input.",
		promptSnippet: "End your turn and sleep until a message for you arrives",
		parameters: Type.Object({}),
		async execute() {
			return {
				content: [{ type: "text", text: "Waiting. You will be woken when a message arrives." }],
				details: {},
				terminate: true,
			};
		},
	});
}
