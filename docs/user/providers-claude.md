# Claude

T3 Code uses Claude Code's login and configuration. Start with the default provider
for one account; [provider setup](./install.md#providers) covers installation and
shared provider settings.

## Separate accounts or configurations

Use a separate Claude config directory for each account. This also works for named
presets that need different Claude settings or a router connection.

Keep your existing account in the default directory. On the environment's machine,
create the second login:

```bash
mkdir -p ~/.claude_personal
CLAUDE_CONFIG_DIR=~/.claude_personal claude auth login
```

Add another Claude instance in **Settings > Providers**:

| Instance        | Binary path | CLAUDE_CONFIG_DIR path |
| --------------- | ----------- | ---------------------- |
| Claude Work     | `claude`    | Leave empty            |
| Claude Personal | `claude`    | `~/.claude_personal`   |

An empty config-directory setting uses Claude Code's normal configuration. The
custom setting changes `CLAUDE_CONFIG_DIR`, leaving `HOME` and the system keychain
location intact. Use the same variable for the login command. Setting `HOME`
instead can put credentials where this provider will not find them.

Check the account reported in provider settings after signing in. Existing
threads can switch only between Claude instances with the same config directory.
Separate account directories stay isolated, including their local conversation
state. Claude does not have Codex's shared-home and shadow-home arrangement.

For presets that differ only in API keys or endpoints, use the instance's
**Environment variables**. Variable assignments do not belong in **Launch arguments**.

Claude Code's verbose mode can stay enabled when you use Claude for text generation, including
thread titles, branch names, commit messages, and pull request descriptions. On a remote connection,
T3 Code uses the Claude configuration on the connected server.

## Compact long conversations

Set **Auto-compact after** in the Claude provider settings to an integer between
`100000` and `1000000`. For example, `300000` asks Claude to summarize at about
300,000 tokens. This changes when compaction happens, not the model's context
window. Leave it empty for Claude Code's default.

You can also send `/compact` in an existing conversation. Web and desktop offer
**Compact context** from the context meter. When you return to a large thread
after Claude's prompt cache expires, a **Compact** chip with the thread's token count shows
next to the send button. While it is on, Enter summarizes the history first, then
sends your message. Click the chip to switch it to **Full** and keep the full
history for that message. The cache lasts about an hour on a Claude
subscription and 5 minutes with an API key, Bedrock, or Vertex. See [commands and skills](./composer.md#commands-and-skills) for using
composer commands.

## Usage limits

If your Claude subscription runs out of usage mid-turn, the thread shows which
limit was reached and the remaining wait when Claude provides a reset time.
Claude Code holds the turn until that window reopens, so it can keep showing as
working. Wait for the reset, or stop the turn and continue later. The warning's
timestamp shows when the displayed wait started.

## Promotional cloud credits (prototype)

Add a separate Claude instance in **Settings > Providers**, then turn on
**Cloud sessions · free credits only (prototype)**. Sign in to the same Claude
subscription in T3's browser using its Default profile. Select that instance,
**Haiku 5.5**, **Opus 5.5**, or **Fable 5.1**, and **Full access** in the composer.
Opus 5.5 is the default. Haiku uses promotional credit more slowly; Fable is also available.
Model changes apply to the next message, including in existing cloud threads.
Fable may require usage credits on your account. If promotional credit cannot
cover a model, leave paid usage disabled; the bridge does not enable it.

Cloud workflows keep their seed titles and automatic worktree names instead of
calling a background text model. The **Text generation model** setting still
applies to ordinary threads.

Cloud launches require unexpired promotional credit and disabled paid usage
credits in Claude's usage settings. The prototype checks these before each
message and during active work. Missing or unknown billing information blocks
launches. API keys and paid API endpoints are unsupported.

The model runs in Anthropic's cloud while T3 tools work in the thread's actual
local project or worktree. Existing edits and untracked files are available;
there is no clean-checkout, GitHub remote, push, or manual synchronization step.
Edits appear in T3's normal local diffs and checkpoints. Browser and thread-history
tools use the same T3 permissions as other providers. Keep T3 and the environment's
machine running while a cloud task uses those tools.

Use T3 delegation for child tasks, scheduled tasks for recurring work, and PR
watching for requested follow-ups. Claude child tasks stay on the same verified
free-credit instance. Every later turn, including automated wake-ups, rechecks
the permitted provider route. Changing a child or scheduled thread to a paid or
unknown route blocks its next turn. Each guarded thread can have four active
delegated tasks, with nesting up to three levels.

To request Codex children, first enable **Allow requested Codex delegation** on the
cloud instance and enable a Codex instance signed in with ChatGPT. Then explicitly
ask Claude to delegate to Codex. These children use the separate Codex subscription;
Claude promotional credit does not pay for them. API-key and custom model routes
are rejected. Descendants inherit the workflow restrictions, so they cannot
change providers or use other model-launch tools to bypass the checks.
Use T3 schedules and PR watching for automation in these Codex children. Native
`/goal` and custom Codex agent configuration files are unavailable.

T3 verifies the selected project before sending each task and supplies its current
repository and worktree context. PR questions use the environment's existing
source-control login; no second GitHub login in the cloud container is needed.
Ask normally: for example, ‘Which recent PRs changed this project's documentation?’

The first response waits for Anthropic's cloud environment to start. A new
worktree also runs the project's setup scripts. Choose **Current checkout** for
repository questions that do not need a separate checkout to avoid that setup.

To switch automatically at a subscription limit, enable **Continue in free cloud
credit when limited** on your ordinary local Claude instance. Keep exactly one
free-credit cloud instance enabled for that same Claude config directory and
account. In a **Full access**, default-mode thread, a rejected usage-limit turn
stops and continues on cloud Opus 5.5 in the same conversation. T3 transfers the
conversation context and keeps the current workspace; the composer follows the
cloud instance for subsequent messages. The free-credit workflow keeps this
thread and its descendants restricted to verified routes. Ambiguous accounts, pending approvals, queued work, snoozed threads, or
unknown billing information block automatic continuation.

For manual switching, stop the current turn, select the cloud instance, and send
a follow-up. You can also start a new cloud thread in any local project.

To continue a cloud task with your local Claude subscription, wait for the current
turn to finish or stop it, select the local Claude instance in the composer, and
send a follow-up yourself. The first switch requires a message from the web,
desktop, or mobile composer; an agent or scheduled task cannot authorize it.
T3 keeps the same thread and worktree and transfers conversation history into a
fresh local Claude session. Long histories may be condensed. Local Claude must
use a built-in model and a verified Pro or Max login for the same account, with
paid extra usage disabled. API routes, custom launch arguments, and Fast mode
are refused. These checks run again before later turns, including automation.
This switch can work after promotional credit is exhausted; it does not happen
automatically. Keep the original cloud instance enabled.

This prototype supports Haiku 5.5, Opus 5.5, Fable 5.1, and Full access. Select
reasoning effort in the composer; T3 confirms that Claude applied it before
sending your message. Fast and Ultracode modes are unavailable.

File tools use paths relative to the thread's workspace. Commands require a Unix
shell and run as persistent jobs with saved output, status, and cancellation.
They have no default execution deadline; an agent can set one explicitly.
Commands continue through a browser or backend reconnect, and stopping a model
turn leaves its jobs running until explicitly cancelled. Agents can read more
output or cancel by job ID instead of starting the command again. Jobs are
noninteractive and do not open a terminal tab or send a completion notification;
the agent checks their status. Local dependencies still run on the environment's
machine.

Cloud-native skills, binary/image attachments, and native conversation fork or
rollback are not fully integrated. Text handoffs are summaries, so they cannot
preserve a provider's hidden reasoning or every detail of a long conversation.

This experimental mode depends on Claude's internal browser interface and may
break when that interface changes. Leave its transport browser tabs alone and
avoid opening the same cloud session in Claude's own interface while T3 is using
it; competing clients can disconnect the tool bridge.

T3 retries interrupted reads without sending your prompt again. If delivery of a
new prompt cannot be confirmed, T3 keeps checking the existing session; it never
resends the prompt automatically. After a backend
restart, a continuation reconnects the existing cloud turn, restores saved
replies and tool state, and collects any remaining output. Completed tool calls
reuse their saved outcomes. Workspace commands reconnect to their persistent
jobs instead of running twice. A graceful shutdown can interrupt the remote
model; recovery collects that result, and you can continue in the same thread.
If automatic recovery fails, resolve the reported issue, then send `/recover`
alone in the same thread. This reconnects the previous cloud turn without sending
another coding prompt. After it finishes, send your next task normally.

A truly uncertain tool side effect, an older journal without recovery data, or missing
cloud history still blocks automatic recovery: inspect the workspace and the
cloud conversation before retrying. Starting another turn does not discard saved
output or repeat an uncertain action.

Promotional credit can expire or become unavailable, at which point the bridge stops instead
of falling back to a paid API. If session creation cannot be confirmed, inspect
Claude Code before retrying; T3 avoids blindly repeating the creation request.

## Skills

Claude skills come from the config directory's `skills` folder and the project's
`.claude/skills` folder. If both define the same name, the config-directory copy
wins. Skills disabled in Claude's settings do not appear in the composer.

Use `$` in the composer to select a skill. Skills marked `disable-model-invocation`
can still be started by you. Invoke those one per message: Claude directly runs
only the last named skill and may try to start earlier ones through its Skill
tool, which refuses skills reserved for manual invocation.

## OpenRouter

Create a Claude instance with its own config directory, such as
`~/.claude_openrouter`, and keep **Binary path** set to `claude`. In that instance's
**Environment variables**, use:

| Variable               | Value                                     |
| ---------------------- | ----------------------------------------- |
| `ANTHROPIC_BASE_URL`   | `https://openrouter.ai/api`               |
| `ANTHROPIC_AUTH_TOKEN` | Your OpenRouter API key, marked Sensitive |
| `ANTHROPIC_API_KEY`    | An explicitly empty value                 |

If that Claude config directory has a cached Anthropic login, run `/logout` in a
Claude Code session using that directory before starting the router setup. Cached
login credentials can conflict with the router token.

Select the model you want in T3 Code. For an OpenRouter model outside the built-in
list, open that Claude instance in **Settings > Providers** and add its full model
ID with **Add custom model**. Then select it in the chat model picker.
`ANTHROPIC_DEFAULT_*_MODEL` variables map Claude Code aliases such as `sonnet`; they
do not replace the explicit model ID selected in T3 Code. Custom models may have
fewer effort, thinking, or context controls than built-in models.

Verify the model used in OpenRouter's activity dashboard. For current compatibility
requirements, use the
[OpenRouter Claude Code guide](https://openrouter.ai/docs/cookbook/coding-agents/claude-code-integration).

## Other routers

A local router uses an ordinary Claude provider instance. Give it a separate
config directory and put the router's endpoint and credential variables in that
instance's **Environment variables**. The router must run where the environment
can reach it. Follow the [Claude Code Router instructions](https://github.com/musistudio/claude-code-router)
for its installation and routing configuration.
