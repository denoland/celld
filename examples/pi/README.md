# PiHarness

This example runs the experimental `PiHarness` from Cloudflare Agents pull
request 2197 in a celld Durable Object. The example calls Cloudflare Workers AI
through the OpenAI-compatible endpoint, and it selects Llama 3.2 1B by default.

Celld serves no Workers AI binding, so the example owns its credentials. It
reads `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN` from the Worker
variables, and it builds the adapter that the pi provider needs. The example
stops at startup when either variable is absent, because a stand-in model
answers every prompt and therefore hides a broken configuration.

The `agents` package on npm does not contain `PiHarness`, therefore this example
depends on a preview build of pull request 2197. The preview build can change or
expire, so an install can fail until that pull request is released.

Install the dependencies, pass the credentials as Worker variables, then start
the example from this directory:

```sh
npm ci

CELLD_VAR_CLOUDFLARE_ACCOUNT_ID="your-account-id" \
CELLD_VAR_CLOUDFLARE_API_TOKEN="your-token" \
celld dev .
```

A `CELLD_VARS_FILE` entry works too, and it keeps the token out of the shell
history.

Run a prompt and inspect the persisted transcript:

```sh
curl http://127.0.0.1:9876/
curl http://127.0.0.1:9876/messages
```

The prompt route answers 200 for a completed run, and 502 with the reported
errors for a failed one.

The Durable Object storage holds the transcript, so the state survives a
restart. A model change does not migrate that state, therefore start again with
`celld dev --clean .` after you edit `PI_MODEL`.
