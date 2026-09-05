# Telemetry

celld can record traces and logs for the requests it serves. The
feature is off by default, and the off state costs nothing. Set
`CELLD_OTEL=1` to turn it on.

The default sink is the fleet bucket. celld writes Parquet files under
the `telemetry/` prefix, so a fleet with a bucket has observability
with no other service. DuckDB can query these files directly. An
alternative sink sends the same data to an OpenTelemetry collector.

The schema is version `v0-unstable`. The column names can change
before a stable release, and each file carries the schema version in
its object metadata.

## Configuration

| variable | default | effect |
| --- | --- | --- |
| `CELLD_OTEL` | `0` | Set this value to `1` to enable telemetry. |
| `CELLD_OTEL_SINK` | `bucket` | `bucket` writes Parquet to the fleet bucket. `otlp` sends OTLP/HTTP protobuf to a collector instead. |
| `CELLD_OTEL_BUCKET` | the fleet bucket | A different bucket for the Parquet files, on the same endpoint and credentials. |
| `CELLD_OTEL_RETENTION` | `30d` | celld deletes telemetry files older than this. `none` disables the deletion, so your own lifecycle rules can control the data. |
| `CELLD_OTEL_FLUSH_MS` | `300000` | celld writes a Parquet file after this many milliseconds of buffered events. |
| `CELLD_OTEL_FLUSH_BYTES` | `5242880` | celld writes a Parquet file after the buffered events reach this many bytes. celld uses the limit it reaches first. |
| `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | A standard sampler name. `traceidratio` with `OTEL_TRACES_SAMPLER_ARG` records a fraction of the traces. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | The collector base URL, for the `otlp` sink. `OTEL_EXPORTER_OTLP_HEADERS` and `OTEL_EXPORTER_OTLP_TIMEOUT` also apply. |
| `OTEL_SERVICE_NAME` | `celld` | The service name in the exported resource. |

The `bucket` sink requires the node to have a fleet bucket
(`CELLD_BUCKET`). The `otlp` sink works on a node without one.

## What celld records

celld records a span for each request a stateless Worker serves, for
each event a cell serves (a fetch, an alarm, an RPC, a WebSocket
message), for each outbound `fetch()`, and for each cell start. A
span carries the request id, the cell, the isolate, the
queue wait, the outbound URL and status, and the durability facts the
runtime already knows.

celld also records each `console.log` line as a log record. The log
record carries the trace id and the span id of the handler that wrote
it, so a query can join the logs to the traces. The correlation
survives `await`.

celld reads the W3C `traceparent` header on incoming requests, so its
spans join the trace of the system in front of it. celld sends a
`traceparent` header on outbound `fetch()`, so downstream systems can
join too. celld ignores a malformed header and starts a new trace. A
Worker call to a Durable Object stays in one trace.

The sampler decides at the start of a request. An unsampled request
records nothing and costs almost nothing. Under load, telemetry sheds
before requests do, and celld counts what it sheds.

A sampling ratio of `0` records no traces, and a ratio of `1` records
every trace. An intermediate ratio makes the same trace-id decision on
each node.

celld keeps a valid incoming context when the sampler rejects it. An
outbound `fetch()` or Durable Object call keeps the trace id, uses a new
span id, and keeps the sampled flag clear.

celld records no metrics yet. The spans carry the durations and the
queue waits, so many questions a metric answers have an answer in the
traces, and a metrics signal can come later without a change to the
trace schema.

## Query the bucket with DuckDB

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET celld_telemetry (
  TYPE s3, KEY_ID '...', SECRET '...',
  ENDPOINT 's3.example.com', URL_STYLE 'path'
);
CREATE VIEW traces AS SELECT * FROM
  read_parquet('s3://YOUR-BUCKET/telemetry/traces/*/*/*/*/*/*.parquet');
CREATE VIEW logs AS SELECT * FROM
  read_parquet('s3://YOUR-BUCKET/telemetry/logs/*/*/*/*/*/*.parquet');

-- The slowest requests.
SELECT name, duration_us, trace_id FROM traces
  ORDER BY duration_us DESC LIMIT 20;

-- Every log line, inside the span that wrote it.
SELECT l.body, t.name, t.duration_us FROM logs l
  JOIN traces t ON l.trace_id = t.trace_id AND l.span_id = t.span_id;
```

An S3-compatible endpoint such as minio needs `URL_STYLE 'path'` in
the secret, and a plain-HTTP endpoint also needs `USE_SSL false`. AWS
itself does not need either.

The files are partitioned by node and by hour:
`telemetry/traces/<node>/<yyyy>/<mm>/<dd>/<hh>/<id>.parquet`. A query
that reads one day therefore touches only that day's files.

## File size and compaction

celld writes one Parquet file for each flush, on a time limit or a
size limit, whichever it reaches first. The defaults are 5 minutes
(`CELLD_OTEL_FLUSH_MS=300000`) and 5 MB (`CELLD_OTEL_FLUSH_BYTES=5242880`).

Keep the defaults if you run no compaction job. They make files large
enough for a fast query with no other moving part. A query sees an
event up to 5 minutes after the request, so the default suits an
investigation after the fact.

Set `CELLD_OTEL_FLUSH_MS=10000` for a near-live view, such as a
dashboard or an active debug session: a query then sees an event
within 10 seconds. A short flush makes many small files, and DuckDB
opens every file a query reads, so you must also run the compaction
job below. Turn on the compaction job first, then shorten the flush,
or queries grow slow within hours.

The `otlp` sink is the other route to a near-live view. It sends each
batch to a collector, so set the same short `CELLD_OTEL_FLUSH_MS`.

The `otlp` sink makes no more than five attempts for a batch when a
transient failure occurs. It uses exponential backoff with jitter, and it
uses an applicable `Retry-After` value from the collector. The exporter caps
each delay at 30 seconds, so a collector cannot suspend a batch indefinitely.
HTTP 408, 429, 502, 503, and 504 responses are transient failures. A
permanent refusal drops the batch immediately.

The exporter owns one retrying batch, and the input channel holds 8192
new events. An outage cannot make either bound grow. celld drops and counts
new telemetry when the channel is full, so request handling continues.

## Compaction

Run a compaction job on a maintenance node, not on a celld node. celld
does not compact its own files, because the serving path must not do
storage maintenance and each node writes only the files it produced.

The job rewrites one past hour of small files into one large file.
DuckDB does the work:

```sql
COPY (
  SELECT * FROM
    read_parquet('s3://YOUR-BUCKET/telemetry/traces/<node>/2026/08/09/22/*.parquet')
  ORDER BY start_unix_us
) TO 's3://YOUR-BUCKET/telemetry/traces/<node>/2026/08/09/22/compacted.parquet'
  (FORMAT parquet, COMPRESSION zstd);
```

Run the job once an hour, for the hour that just ended. Do not compact
the current hour, because a node still writes to it. Delete the source
files after DuckDB writes the compacted file. The compacted file is
also smaller, because zstd compresses one sorted batch better than
many separate files.
