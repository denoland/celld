# OpenCode Workerd SDK

This example runs the OpenCode Workerd SDK in a Durable Object. The Durable
Object storage holds the SDK state, so a session survives a restart. The example
creates a session, sends a prompt, waits for the run to complete, and returns
the assistant reply.

The example needs no credentials. The SDK resolves a free model from its own
provider registry, and it reports the model that answered.

Install the example dependencies with `npm ci`, then start it:

```sh
celld dev .
curl http://127.0.0.1:9876/
```

A successful response contains the reply and the model that produced it:

```json
{
  "ok": true,
  "model": {
    "providerID": "opencode",
    "modelID": "nemotron-3.5-lightning-free"
  },
  "prompt": "hello world",
  "reply": "Hello! How can I help you today?",
  "checks": [{ "name": "model.default", "ok": true, "ms": 12 }]
}
```

The response has status 502 when the run completes but no assistant reply
arrives, and status 500 when an SDK call fails. Therefore a broken model path
fails the probe instead of passing it.
