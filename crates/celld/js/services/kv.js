// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The KV service, shared by both engines: the namespace cell (the server) and
// the binding's client. A KV namespace is a cell of a runtime-supplied Durable
// Object class, so it inherits ownership, fencing, LTX replication and durable
// acknowledgement from the cell it already is.
//
// Reads here are strongly consistent and the published contract is upstream's:
// a value can be up to 60 seconds old. That gap is deliberate. A node-local
// read cache is the obvious next optimisation and it would make reads
// genuinely stale, and a freshness promise made now could not be withdrawn
// then. Implement strong, promise weak.
//
// Storage is ordinary cell SQL rather than the KV surface, because a namespace
// is read by ordered prefix scan and `list` is the operation that decides the
// schema. `celld_logic::kv` owns every decision about a key, a deadline or a
// limit; nothing here re-derives one.
//
// A function declaration, so the V8 harness (one function body) and the
// workerd module can each append this text and call it from above. `host` is
// what differs between the engines:
//
// - `DurableObject`: the cell's base class. workerd's gives a cell RPC; the
//   V8 harness grants that to its runtime classes instead.
// - `limits()`: every bound `celld_logic::kv` declares, plus the test knobs.
// - `blob(cell, request, value)`: the large-value path (prepare, put, get,
//   sweep) to the fleet bucket, with the cell's activation epoch as the
//   authority. Bytes cross as a typed view in both directions: encoding a
//   25 MiB value as a JSON number array creates millions of heap objects and
//   can crash an otherwise valid put. The answer is `{ found: true, value }`
//   for bytes and the parsed JSON reply otherwise.
// - `btoa(bytes)`, `atob(text)`: base64 of bytes and back, for the operator
//   route.
function kvService(host) {
  // Run one bounded reclamation transaction. A cell supplies only its ordered
  // candidate query and its per-row transition, so KV expiry and Queue retention
  // share the turn bound and the transaction shape without pretending their SQL
  // state machines are the same.
  const __cellSweepBatch = (storage, select, reclaim, limit) => {
    let processed = 0;
    storage.transactionSync(() => {
      const rows = select(limit);
      if (rows.length > limit) throw new Error("a cell sweep exceeded its row bound");
      for (const row of rows) {
        reclaim(row);
        processed += 1;
      }
    });
    return processed;
  };

  const __KV_TABLE = "__kv";
  const __KV_META_TABLE = "__kv_meta";

  // Upstream's four content types. The wire form is what `get` returns for
  // `type: "text"`; the binding converts from there, so the cell stores bytes
  // and a tag and never a parsed value.
  const __kvError = (message) => new Error("KV_ERROR: " + String(message));

  // The authenticated operator route still uses JSON for its control envelope.
  // A byte array is acceptable for an inline value, but a 25 MiB array creates
  // 25 million JavaScript numbers and exhausts the isolate. The raw base64 ops
  // accept typed views for this internal wire form; the public atob() and btoa()
  // wrappers keep their standard string-only behaviour.
  const __kvOperatorValue = (value) => {
    const bytes = value instanceof Uint8Array ? value : new Uint8Array(value);
    return bytes.byteLength > __kvLimits().maxInlineValueBytes
      ? { value: host.btoa(bytes), valueEncoding: "base64" }
      : { value: [...bytes] };
  };

  // Content addressing, so the same value written twice in one ownership epoch
  // stores once and a retry after an uncertain failure costs nothing. A later
  // epoch deliberately uses a different object. SHA-256 makes an accidental
  // collision infeasible. The host adds the active cell scope and epoch to the
  // object key, because one activation cannot prove another activation's live
  // set and therefore cannot safely collect from a shared digest prefix.
  const __kvDigest = async (bytes) => {
    const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
    let out = "";
    for (const byte of hash) out += byte.toString(16).padStart(2, "0");
    return out;
  };

  // The least name that sorts after every name with this prefix, or null when no
  // such name exists. SQLite compares TEXT byte by byte and UTF-8 byte order is
  // code point order, so the successor is the prefix with its final code point
  // incremented; a final code point that is already the highest carries into the
  // code point before it, and an empty prefix has no upper bound at all.
  //
  // A lone surrogate has no UTF-8 form, so a prefix that holds one gets no bound
  // rather than a bound derived from whatever the host substitutes. Such a
  // prefix matches no stored name, and answering it with a wrong range would
  // answer it with the wrong keys instead of slowly.
  const __kvPrefixUpperBound = (prefix) => {
    if (/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/.test(prefix)) {
      return null;
    }
    // Array.from splits on code points, so a surrogate pair stays one element
    // and cannot be incremented into a different character.
    const points = Array.from(prefix);
    while (points.length > 0) {
      const code = points.pop().codePointAt(0);
      if (code >= 0x10FFFF) continue;
      // The surrogate block is unencodable, so step over it in one move.
      const next = code + 1 === 0xD800 ? 0xE000 : code + 1;
      return points.join("") + String.fromCodePoint(next);
    }
    return null;
  };

  // Which rows a list visits is not visible in its result, so a lost bound reads
  // as a correct answer and is measurable only as latency. The plan is the
  // evidence, and a gated test seam reads it from the shipped query rather than
  // from a copy that can drift.
  const __kvReportListPlan = (sql, query, params) => {
    if (typeof __test_kv_list_plan !== "function") return;
    const plan = sql.exec(`EXPLAIN QUERY PLAN ${query}`, ...params)
      .toArray()
      .map((row) => row.detail)
      .join("; ");
    __test_kv_list_plan(plan);
  };

  class __KvNamespaceCell extends host.DurableObject {
    constructor(ctx, env) {
      super(ctx, env);
      this._ready = false;
      // Blob references written to the bucket whose row has not committed yet. A sweep
      // must spare these, or it collects bytes a put is about to reference.
      this._pending = new Set();
      // Blob writes and the mark-and-sweep protocol share one per-cell queue.
      // The pending set is live state, not a snapshot that stays valid across an
      // await, so collection cannot overlap a put from its announcement through
      // its row commit. Other cells and ordinary reads stay independent.
      this._blobProtocolTail = Promise.resolve();
    }

    async _withBlobProtocol(operation) {
      const previous = this._blobProtocolTail;
      let release;
      this._blobProtocolTail = new Promise((resolve) => {
        release = resolve;
      });
      await previous;
      try {
        return await operation();
      } finally {
        release();
      }
    }

    // Schedule collection before a blob can leave the process or a row can drop
    // its reference. `setAlarm()` writes cell state, and the following bucket
    // egress waits on that write's output gate. A crash can therefore leave an
    // orphan only together with the durable wake that will collect it.
    async _armBlobSweep() {
      this._open().exec(
        `INSERT INTO ${__KV_META_TABLE} (name, value) VALUES ('blob-sweep-due', 1)
         ON CONFLICT(name) DO UPDATE SET value = excluded.value`,
      );
      const now = Date.now();
      const deadline = now + __kvLimits().blobSweepMs;
      const armed = await this.ctx.storage.getAlarm();
      // During an alarm event, `getAlarm()` can still report the timestamp that
      // caused the current delivery. It is not a future wake and cannot cover a
      // reference removed by an event that interleaves with this one.
      if (armed === null || armed <= now || armed > deadline) {
        await this.ctx.storage.setAlarm(deadline);
      }
    }

    _blobSweepDue() {
      return this._open().exec(
        `SELECT 1 AS due FROM ${__KV_META_TABLE}
          WHERE name = 'blob-sweep-due' AND value = 1`,
      ).toArray().length > 0;
    }

    _clearBlobSweepDue() {
      this._open().exec(
        `DELETE FROM ${__KV_META_TABLE} WHERE name = 'blob-sweep-due'`,
      );
    }

    // Created on first touch rather than at construction, so a namespace that is
    // only ever read costs no write, and an eviction that drops the isolate does
    // not need the table rebuilt before the cell can answer.
    _open() {
      if (this._ready) return this.ctx.storage.sql;
      const sql = this.ctx.storage.sql;
      // Recreate the first release's table before the normal open path. The
      // migration test then reaches the same `CREATE IF NOT EXISTS` boundary as
      // an upgraded cell and starts with a row it must preserve.
      if (__kvLimits().legacySchema) {
        sql.exec(
          `CREATE TABLE IF NOT EXISTS ${__KV_TABLE} (
             name TEXT PRIMARY KEY,
             value BLOB,
             tag TEXT NOT NULL,
             metadata TEXT,
             expires_at INTEGER
           ) WITHOUT ROWID`,
        );
        sql.exec(
          `INSERT OR IGNORE INTO ${__KV_TABLE}
             (name, value, tag, metadata, expires_at)
           VALUES ('before-upgrade', CAST('legacy' AS BLOB), 'text', NULL, NULL)`,
        );
      }
      sql.exec(
        `CREATE TABLE IF NOT EXISTS ${__KV_TABLE} (
           name TEXT PRIMARY KEY,
           value BLOB,
           blob_id TEXT,
           size INTEGER NOT NULL,
           tag TEXT NOT NULL,
           metadata TEXT,
           expires_at INTEGER
         ) WITHOUT ROWID`,
      );
      // Inline KV shipped before `blob_id` and `size`. `CREATE IF NOT EXISTS`
      // does not change that table, so migrate each column independently. A
      // crash between the ALTER statements is safe because the next open reads
      // the surviving shape and resumes at the missing column.
      const columns = new Set(
        sql.exec(`PRAGMA table_info(${__KV_TABLE})`).toArray().map((row) => row.name),
      );
      if (!columns.has("blob_id")) {
        sql.exec(`ALTER TABLE ${__KV_TABLE} ADD COLUMN blob_id TEXT`);
      }
      if (!columns.has("size")) {
        sql.exec(
          `ALTER TABLE ${__KV_TABLE}
             ADD COLUMN size INTEGER NOT NULL DEFAULT 0`,
        );
        sql.exec(`UPDATE ${__KV_TABLE} SET size = LENGTH(value)`);
      }
      // `list` walks the primary key in order, so the only index worth carrying
      // is the sweeper's. Without it the sweep is a full scan of a namespace
      // whose whole point is being large.
      sql.exec(
        `CREATE INDEX IF NOT EXISTS ${__KV_TABLE}_expires
           ON ${__KV_TABLE} (expires_at) WHERE expires_at IS NOT NULL`,
      );
      // The due bit is durable because the isolate that wrote a blob can die
      // before its row commit. In-memory state cannot schedule that orphan's
      // collector on the next owner.
      sql.exec(
        `CREATE TABLE IF NOT EXISTS ${__KV_META_TABLE} (
           name TEXT PRIMARY KEY,
           value INTEGER NOT NULL
         ) WITHOUT ROWID`,
      );
      this._ready = true;
      return sql;
    }

    // A key past its deadline is invisible from the instant it expires, not from
    // whenever the sweeper next runs. The read path filters and the sweep only
    // reclaims space, so the two can never disagree about which side of the
    // boundary a key is on.
    async __kvGet({ keys, withMetadata, withExpiration = false, now }) {
      const sql = this._open();
      const out = [];
      for (const key of keys) {
        const row = sql.exec(
          `SELECT value, blob_id, tag, metadata, expires_at FROM ${__KV_TABLE}
            WHERE name = ? AND (expires_at IS NULL OR expires_at > ?)`,
          key,
          now,
        ).toArray()[0];
        if (row === undefined) {
          out.push({ key, found: false });
          continue;
        }
        let value = row.value;
        if (row.blob_id !== null && row.blob_id !== undefined) {
          const blob = await host.blob(this, { mode: "get", reference: row.blob_id });
          // A row naming a blob the bucket does not hold is the failure the
          // commit order exists to prevent, so it is reported as itself rather
          // than as a missing key: an absent key and a broken one need different
          // answers from an operator.
          if (!blob.found) {
            throw __kvError(
              `the value for ${JSON.stringify(key)} is missing from the fleet ` +
                `bucket (blob ${row.blob_id})`,
            );
          }
          value = new Uint8Array(blob.value);
        }
        const entry = { key, found: true, value, tag: row.tag };
        if (withMetadata) entry.metadata = row.metadata ?? null;
        // These fields describe one row snapshot. The operator bulk export must
        // not combine a value from this read with an expiration from an earlier
        // listing when a concurrent put replaces the key between both calls.
        if (withExpiration) entry.expiration = row.expires_at ?? null;
        out.push(entry);
      }
      return out;
    }

    // One transaction for the whole batch, so a bulk put is all-or-nothing
    // rather than a prefix of itself. Upstream gives no such guarantee, and
    // giving a stronger one here costs nothing and cannot surprise a caller.
    // Blob first, row second. An orphan blob is bytes the collector reclaims; a
    // row naming a blob that was never written is a read that fails forever, and
    // no sweep repairs it. The opposite commit order can leave that dangling
    // row when a process stops between the two writes.
    //
    // The blob reference travels in `_pending` until the row commits, so a sweep that
    // runs mid-put does not collect bytes that are about to be referenced. That
    // costs the put and not the read -- the commit requires the blob, so a
    // collected blob means the commit cannot fire -- which is why the model's
    // tooth for it is an action property and not an invariant.
    async _store(value) {
      if (value.byteLength <= __kvLimits().maxInlineValueBytes) {
        return { value, blobId: null };
      }
      const digest = await __kvDigest(value);
      await this._armBlobSweep();
      // The host mints the reference from the epoch installed with this cell.
      // JavaScript never guesses that authority or recovers it after an await.
      const { reference } = await host.blob(this, { mode: "prepare", digest });
      this._pending.add(reference);
      try {
        await host.blob(this, { mode: "put", reference }, value);
        // Force the collector race at its real seam. The alarm took its mark
        // snapshot before this put started and waits briefly after the put
        // announces itself. Without blob-protocol serialization, this write
        // lands during that wait and must stay here until the stale sweep runs.
        if (
          __kvLimits().raceSweepPut && this._testSweepMarked &&
          !this._testSweepFinished
        ) {
          await new Promise((resolve) => {
            this._testResumePut = resolve;
          });
        }
        // The crash window the commit order exists for: the blob is in the
        // bucket and the row is not written yet. The failed operation no longer
        // protects the digest in memory, so the durable wake can reclaim it even
        // when this isolate remains resident.
        if (__kvLimits().failAfterBlobWrite) {
          throw __kvError("CELLD_TEST_KV_FAIL_AFTER_BLOB_WRITE");
        }
        return { value: null, blobId: reference };
      } catch (error) {
        this._pending.delete(reference);
        throw error;
      }
    }

    async __kvPut({ entries }) {
      if (
        __kvLimits().raceSweepPut &&
        entries.some((entry) =>
          entry.value.byteLength > __kvLimits().maxInlineValueBytes
        )
      ) {
        if (!this._testSweepMarked || this._testPutAttempted === undefined) {
          throw __kvError("the test blob sweep did not reach its mark snapshot");
        }
        this._testPutAttempted();
        this._testPutAttempted = undefined;
      }
      return this._withBlobProtocol(() => this._putLocked(entries));
    }

    async _putLocked(entries) {
      const sql = this._open();
      // An inline replacement can remove the final row reference to an old
      // blob, so arm before changing the row. A new large value also arms in
      // `_store`, before its own bucket write.
      const replacesBlob = entries.some((entry) =>
        sql.exec(
          `SELECT 1 AS found FROM ${__KV_TABLE}
            WHERE name = ? AND blob_id IS NOT NULL`,
          entry.key,
        ).toArray().length > 0
      );
      if (replacesBlob) await this._armBlobSweep();
      const stored = [];
      try {
        for (const entry of entries) {
          stored.push({ entry, ...(await this._store(entry.value)) });
        }
        this.ctx.storage.transactionSync(() => {
          for (const { entry, value, blobId } of stored) {
            sql.exec(
              `INSERT INTO ${__KV_TABLE}
                 (name, value, blob_id, size, tag, metadata, expires_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET
                 value = excluded.value,
                 blob_id = excluded.blob_id,
                 size = excluded.size,
                 tag = excluded.tag,
                 metadata = excluded.metadata,
                 expires_at = excluded.expires_at`,
              entry.key,
              value,
              blobId,
              entry.value.byteLength,
              entry.tag,
              entry.metadata ?? null,
              entry.expiresAt ?? null,
            );
          }
        });
      } finally {
        // A committed row protects the blob durably. A failed put protects
        // nothing, so it must not pin an orphan until this isolate is evicted.
        for (const { blobId } of stored) {
          if (blobId !== null) this._pending.delete(blobId);
        }
      }
      // Only when something in this batch can expire. A namespace of permanent
      // keys arms no alarm and wakes for nothing.
      if (entries.some((entry) => entry.expiresAt !== null && entry.expiresAt !== undefined)) {
        await this._rearm();
      }
    }

    async __kvDelete({ keys }) {
      return this._withBlobProtocol(() => this._deleteLocked(keys));
    }

    async _deleteLocked(keys) {
      const sql = this._open();
      const removesBlob = keys.some((key) =>
        sql.exec(
          `SELECT 1 AS found FROM ${__KV_TABLE}
            WHERE name = ? AND blob_id IS NOT NULL`,
          key,
        ).toArray().length > 0
      );
      if (removesBlob) await this._armBlobSweep();
      this.ctx.storage.transactionSync(() => {
        for (const key of keys) {
          sql.exec(`DELETE FROM ${__KV_TABLE} WHERE name = ?`, key);
        }
      });
    }

    // Arm for the earliest deadline the table holds, or disarm when it holds
    // none. Asked from the row rather than held in memory, so the answer does
    // not depend on what this activation happens to remember -- an evicted cell
    // that wakes for an alarm re-derives the same deadline from the same rows.
    //
    // What carries the deadline across an eviction is the alarm itself, which is
    // durable cell state. This is *not* recomputed when a cell is merely opened,
    // so the one gap is a `_rearm` that throws after its put committed: the rows
    // are written, the caller sees an error, and nothing is armed until the next
    // put with a deadline. That costs space and never correctness, because the
    // read path filters an expired key whether or not it was reclaimed.
    async _rearm() {
      // A test that means to pin the read filter must be able to stop the sweep,
      // or it cannot tell which mechanism hid the key -- and a test that cannot
      // tell will pass when the filter is broken. This seam exists because that
      // is exactly what happened: the first version of the expiry test stayed
      // green with the filter removed.
      if (__kvLimits().sweepDisabled) return;
      const sql = this._open();
      const row = sql.exec(
        `SELECT MIN(expires_at) AS next FROM ${__KV_TABLE} WHERE expires_at IS NOT NULL`,
      ).toArray()[0];
      const next = row === undefined ? null : row.next;
      const blobDue = this._blobSweepDue();
      if ((next === null || next === undefined) && !blobDue) {
        await this.ctx.storage.deleteAlarm();
        return;
      }
      const now = Date.now();
      const deadlines = [];
      // Never in the past: a deadline already due is swept on this wake, and
      // arming behind `now` would spin.
      if (next !== null && next !== undefined) {
        deadlines.push(Math.max(next, now + 1));
      }
      if (blobDue) deadlines.push(now + __kvLimits().blobSweepMs);
      const deadline = Math.min(...deadlines);
      const armed = await this.ctx.storage.getAlarm();
      if (armed === null || armed <= now || armed > deadline) {
        await this.ctx.storage.setAlarm(deadline);
      }
    }

    // Reclaiming space, never deciding visibility. A key is invisible from the
    // instant it expires because the read path filters, so a late sweep costs
    // storage and never correctness -- which is what lets this be bounded and
    // re-armed rather than obliged to finish.
    // Every reference a row names, plus every reference a put has written and
    // not yet committed. The two go in one list because the bucket end cannot
    // tell them apart and does not need to: it only needs "do not delete these".
    _liveBlobs() {
      const rows = this._open().exec(
        `SELECT blob_id FROM ${__KV_TABLE} WHERE blob_id IS NOT NULL`,
      ).toArray();
      return [...new Set([...rows.map((row) => row.blob_id), ...this._pending])];
    }

    async alarm() {
      const removed = await this._withBlobProtocol(async () => {
        const now = Date.now();
        // If expiry will remove a blob reference, persist the due bit and a
        // future wake before deleting the row. A crash later in this alarm then
        // leaves the next owner enough durable state to finish collection.
        const expiresBlob = this._open().exec(
          `SELECT 1 AS found FROM ${__KV_TABLE}
            WHERE expires_at IS NOT NULL AND expires_at <= ?
              AND blob_id IS NOT NULL
            LIMIT ?`,
          now,
          __kvLimits().sweepBatchRows,
        ).toArray().length > 0;
        if (expiresBlob) await this._armBlobSweep();

        const { removed } = this.__kvSweep({
          now,
          limit: __kvLimits().sweepBatchRows,
        });
        // The blob sweep runs after the row sweep, on the same wake, so a row
        // reclaimed above has already dropped its reference by the time the live
        // set is read. A durable due bit distinguishes a GC wake from an inline
        // expiry wake and survives isolate eviction or a failed bucket request.
        if (this._blobSweepDue() || __kvLimits().raceSweepPut) {
          const live = this._liveBlobs();
          if (__kvLimits().raceSweepPut && !this._testSweepFinished) {
            const putAttempted = new Promise((resolve) => {
              this._testPutAttempted = resolve;
            });
            this._testSweepMarked = true;
            await putAttempted;
            // The fixture bucket is local. This bounded pause lets an
            // unprotected put finish its blob write; a protected put is waiting
            // for this protocol lock and therefore cannot enter the window.
            await new Promise((resolve) => setTimeout(resolve, 100));
          }
          let swept = false;
          try {
            await host.blob(this, { mode: "sweep", live });
            swept = true;
          } catch (error) {
            // Reclaiming space is not deciding visibility, so a bucket that
            // refuses a delete costs storage and never correctness. The next
            // wake tries again.
            console.error("kv blob sweep failed:", error);
          } finally {
            if (__kvLimits().raceSweepPut) {
              this._testSweepFinished = true;
              this._testResumePut?.();
              this._testResumePut = undefined;
            }
          }
          if (swept) this._clearBlobSweepDue();
        }
        return removed;
      });
      // A full batch means there is more to reclaim, so come back promptly
      // rather than waiting for the next natural deadline.
      if (removed >= __kvLimits().sweepBatchRows) {
        await this.ctx.storage.setAlarm(Date.now() + 1);
        return;
      }
      await this._rearm();
    }

    // Pagination resumes at `name > after`, never at an offset. A caller holds
    // no transaction across pages, so an offset would skip or repeat keys the
    // moment a concurrent writer inserted or deleted inside the prefix.
    //
    // One row beyond the limit is read to decide `list_complete`, and discarded.
    // Asking the database is the only honest answer: a page that happens to fill
    // exactly is indistinguishable from a page that ended.
    __kvList({ prefix, limit, after, now }) {
      const sql = this._open();
      // Every name a prefix can match is one contiguous run of the primary key,
      // so bound the key on both sides. The query used to carry the cursor as
      // its only range, and SQLite then walked every earlier row in the
      // namespace before it could report that a late prefix matches nothing: an
      // empty-result list measured 684/s over 4,849 rows and 43/s over 114,881.
      //
      // The bounds decide which rows a list visits and the prefix comparison
      // still decides which rows match. A prefix whose bounds cannot be derived
      // -- one holding a lone surrogate, which no stored name can hold -- is
      // therefore answered at the old cost rather than short of keys.
      const upper = __kvPrefixUpperBound(prefix);
      // A cursor names a key this namespace already returned for this prefix, so
      // it is inside the range and replaces the prefix as the start. Carrying
      // both would leave SQLite to pick one as the seek and demote the other to
      // a filter, and picking the prefix rescans every earlier page. A cursor
      // arrives from the caller, so a hand-made one can start below the prefix;
      // the comparison below, and not the range, keeps that page correct.
      const start = after ? "name > ?" : "name >= ?";
      const params = [after ? after : prefix];
      if (upper !== null) params.push(upper);
      // `substr` compares under BINARY, so a prefix matches by bytes and not by
      // ASCII case. `LIKE` stood here and folded case, which made `list({prefix:
      // "A"})` answer a key named `a` -- upstream compares a byte prefix, and no
      // range bound can reproduce a case-folded match anyway. The length counts
      // code points, which is what SQLite counts, not UTF-16 units.
      if (prefix !== "") params.push(Array.from(prefix).length, prefix);
      const query = `SELECT name, metadata, expires_at FROM ${__KV_TABLE}
          WHERE ${start}
            ${upper === null ? "" : "AND name < ?"}
            ${prefix === "" ? "" : "AND substr(name, 1, ?) = ?"}
            AND (expires_at IS NULL OR expires_at > ?)
          ORDER BY name
          LIMIT ?`;
      params.push(now, limit + 1);
      __kvReportListPlan(sql, query, params);
      const rows = sql.exec(query, ...params).toArray();
      const complete = rows.length <= limit;
      const page = complete ? rows : rows.slice(0, limit);
      return {
        keys: page.map((row) => ({
          name: row.name,
          metadata: row.metadata ?? null,
          expiration: row.expires_at ?? null,
        })),
        complete,
      };
    }

    // The operator's live key and byte counts. `size` stays in the row because
    // a bucket-backed value has a NULL inline `value`; `LENGTH(value)` would
    // report zero bytes for exactly the values whose size matters most.
    __kvMetrics({ now }) {
      const sql = this._open();
      const row = sql.exec(
        `SELECT COUNT(*) AS count,
                COALESCE(SUM(size), 0) AS bytes
           FROM ${__KV_TABLE}
          WHERE expires_at IS NULL OR expires_at > ?`,
        now,
      ).toArray()[0];
      return { count: row.count, bytes: row.bytes };
    }

    // Reclaim what the read path already treats as gone. Bounded per call so a
    // namespace with a large expired population cannot hold the cell for an
    // unbounded time; the caller re-arms while there is more.
    __kvSweep({ now, limit }) {
      const sql = this._open();
      const removed = __cellSweepBatch(
        this.ctx.storage,
        (bound) => sql.exec(
          `SELECT name FROM ${__KV_TABLE}
            WHERE expires_at IS NOT NULL AND expires_at <= ?
            ORDER BY expires_at
            LIMIT ?`,
          now,
          bound,
        ).toArray(),
        (row) => {
          sql.exec(`DELETE FROM ${__KV_TABLE} WHERE name = ?`, row.name);
        },
        limit,
      );
      return { removed };
    }

    // The operator surface, reached only over `/runtime/`, which authenticates
    // with the fleet secret. `/do/` refuses every reserved class structurally,
    // so adding this handler does not put a namespace on an unauthenticated
    // route -- the trap d1.md decision 4 records paying for once, and the reason
    // that refusal is one question rather than one per class.
    //
    // Values cross this boundary as arrays of bytes rather than as text, because
    // a namespace holds bytes: a value written from a Worker as an ArrayBuffer
    // has no faithful string form, and `celld kv` must be able to read back
    // exactly what was written.
    async fetch(request) {
      let body;
      try {
        body = await request.json();
      } catch {
        return Response.json({ error: "KV_ERROR: invalid request body" }, { status: 400 });
      }
      const now = Date.now();
      try {
        let result;
        switch (body.op) {
          case "get": {
            // Awaited: `__kvGet` resolves a bucket-backed value, so it is
            // async. Destructuring the promise instead threw "object is not
            // iterable" on every operator read, and only here -- the binding
            // reaches the same method over RPC, which awaits for it.
            const [row] = await this.__kvGet({
              keys: [String(body.key)],
              withMetadata: true,
              withExpiration: true,
              now,
            });
            result = row.found
              ? {
                found: true,
                ...__kvOperatorValue(row.value),
                tag: row.tag,
                metadata: row.metadata ?? null,
                expiration: row.expiration,
              }
              : { found: false };
            break;
          }
          case "put":
          case "put-base64": {
            // Validated here, through the same helpers the binding calls, so a
            // key written by `celld kv` is a key the binding could have written.
            // The operator route used to arrive pre-validated by the CLI, which
            // put the same bound in two processes -- and they disagreed the first
            // time a test shortened one of them.
            const key = String(body.key);
            __kvCheckKey(key);
            const value = body.op === "put-base64"
              ? host.atob(String(body.value))
              : new Uint8Array(body.value);
            __kvCheckValue(value.byteLength);
            const metadata = body.metadata === undefined ? null : body.metadata;
            if (metadata !== null) __kvCheckMetadata(metadata);
            // Resolved from the cell's clock, not the caller's, because that is
            // the clock the read filter and the sweeper compare against.
            const expiresAt = __kvExpiryAt(now, body.expiration, body.expirationTtl);
            await this.__kvPut({
              entries: [{
                key,
                value,
                tag: String(body.tag ?? __KV_TAG_BYTES),
                metadata,
                expiresAt,
              }],
            });
            result = { ok: true };
            break;
          }
          case "delete":
            for (const key of body.keys) __kvCheckKey(String(key));
            await this.__kvDelete({ keys: body.keys.map(String) });
            result = { ok: true };
            break;
          case "list":
            result = this.__kvList({
              prefix: String(body.prefix ?? ""),
              limit: Number(body.limit),
              after: String(body.after ?? ""),
              now,
            });
            break;
          // What `celld kv info` reports. `stored` counts every row and `live`
          // counts only what a read would return, so the difference is exactly
          // the population the sweeper still owes -- which is the one number
          // that makes reclamation observable from outside the cell.
          case "info": {
            const live = this.__kvMetrics({ now });
            const total = this._open().exec(
              `SELECT COUNT(*) AS count FROM ${__KV_TABLE}`,
            ).toArray()[0];
            result = { live: live.count, bytes: live.bytes, stored: total.count };
            break;
          }
          default:
            return Response.json(
              { error: `KV_ERROR: unknown operation ${JSON.stringify(body.op)}` },
              { status: 400 },
            );
        }
        return Response.json({ result });
      } catch (error) {
        return Response.json(
          { error: String(error && error.message || error) },
          { status: 400 },
        );
      }
    }
  }

  // The client half.
  //
  // Every number below is injected from `celld_logic::kv` as `host.limits()`
  // rather than written here, so the binding, `celld kv` and deploy-time
  // validation cannot disagree about what a valid key is. No new host op: D1 set
  // the bar at one and Workflows came in under it, and a limit is data rather
  // than a decision, so shipping the values is enough. What stays in JS is the
  // presentation -- the error text is the binding's published contract, in
  // upstream's `KV <OP> failed:` shape, and belongs beside the calls that raise
  // it.
  //
  // The cursor codec is the one thing that must byte-match Rust's, and it is
  // plain hex: a total function with a single right answer, which two correct
  // implementations cannot disagree about. A policy would be a different matter
  // and none is duplicated here.
  const __kvLimits = host.limits;

  const __kvCheckKey = (key) => {
    if (key.length === 0) throw __kvError("a key must not be empty");
    const bytes = __kvEncoder.encode(key).byteLength;
    if (bytes > __kvLimits().maxKeyBytes) {
      throw __kvError(
        `a key is at most ${__kvLimits().maxKeyBytes} bytes, got ${bytes}`,
      );
    }
  };

  // A TTL becomes an absolute deadline here, once, at the call. Resolving it
  // again later against a newer clock would extend the life of the key every
  // time anything re-read the row -- the reason a workflow persists a sleep
  // deadline and not its duration.
  const __kvCheckValue = (size) => {
    // Upstream's bound, and now the only one. A value above the inline bound is
    // no longer refused: it goes to the fleet bucket, which is the split
    // Cloudflare's own KV rearchitecture made and for the same reason -- a cell
    // replicates every write as LTX, so an inline value is paid for twice.
    if (size > __kvLimits().maxValueBytes) {
      throw __kvError(`a value is at most ${__kvLimits().maxValueBytes} bytes, got ${size}`);
    }
  };

  const __kvCheckMetadata = (metadata) => {
    const bytes = __kvEncoder.encode(metadata).byteLength;
    if (bytes > __kvLimits().maxMetadataBytes) {
      throw __kvError(
        `metadata is at most ${__kvLimits().maxMetadataBytes} bytes, got ${bytes}`,
      );
    }
  };

  const __kvExpiryAt = (now, expiration, expirationTtl) => {
    if (expirationTtl !== undefined && expirationTtl !== null) {
      const ms = Math.floor(Number(expirationTtl) * 1000);
      if (!Number.isFinite(ms) || ms < __kvLimits().minExpirationTtlMs) {
        throw __kvError(
          `expirationTtl is at least ${__kvLimits().minExpirationTtlMs / 1000} seconds`,
        );
      }
      return now + ms;
    }
    if (expiration !== undefined && expiration !== null) {
      const at = Math.floor(Number(expiration) * 1000);
      if (!Number.isFinite(at) || at <= now) {
        throw __kvError(`expiration ${expiration} is in the past`);
      }
      return at;
    }
    return null;
  };

  const __kvCursor = {
    encode(key) {
      let out = "";
      for (const byte of __kvEncoder.encode(key)) {
        out += byte.toString(16).padStart(2, "0");
      }
      return out;
    },
    decode(cursor) {
      // A malformed cursor is refused, never read as "start from the
      // beginning": that would hand a paginating caller its first page a second
      // time and call it progress.
      if (cursor.length % 2 !== 0 || /[^0-9a-f]/.test(cursor)) {
        throw __kvError("the list cursor is malformed");
      }
      const bytes = new Uint8Array(cursor.length / 2);
      for (let at = 0; at < bytes.length; at += 1) {
        bytes[at] = Number.parseInt(cursor.slice(at * 2, at * 2 + 2), 16);
      }
      return __kvDecoder.decode(bytes);
    },
  };

  const __kvListLimit = (requested) => {
    const max = __kvLimits().maxListLimit;
    if (requested === undefined || requested === null) return max;
    const limit = Math.floor(Number(requested));
    // Zero would be a page that never ends.
    if (!Number.isFinite(limit) || limit <= 0) return max;
    return Math.min(limit, max);
  };

  // Upstream's four content types, and what each one means on the way in and out.
  const __KV_TAG_TEXT = "text";
  const __KV_TAG_BYTES = "bytes";

  const __kvEncoder = new TextEncoder();
  const __kvDecoder = new TextDecoder();

  // A value becomes bytes plus a tag, once, at the public boundary. Storing the
  // tag is what lets `get(key, "text")` and `get(key)` answer differently from
  // the same row without the cell parsing anything.
  const __kvEncodeValue = (value) => {
    if (typeof value === "string") {
      return { value: __kvEncoder.encode(value), tag: __KV_TAG_TEXT };
    }
    if (value instanceof ArrayBuffer) {
      return { value: new Uint8Array(value.slice(0)), tag: __KV_TAG_BYTES };
    }
    if (ArrayBuffer.isView(value)) {
      // Copied, not referenced. `sendBatch`'s resizable-ArrayBuffer regression
      // upstream is the same hazard: a caller may resize or reuse the buffer
      // between this call and the write, and a shallow view would then read
      // decommitted pages.
      return {
        value: new Uint8Array(
          value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength),
        ),
        tag: __KV_TAG_BYTES,
      };
    }
    throw __kvError(
      "a KV value must be a string, an ArrayBuffer, a typed array, or a ReadableStream",
    );
  };

  // Upstream accepts a ReadableStream, and `put(key, request.body)` is how a
  // handler stores a request body, so refusing one broke the obvious spelling.
  // The stream must become bytes before the cell write: a cell replicates a
  // value as LTX, and a pending stream has no bytes to replicate.
  //
  // The bound is enforced while draining rather than on the finished buffer.
  // Checking afterwards would let a body larger than the limit reach memory in
  // full before the refusal, which hands an unbounded allocation to whoever
  // sends the request.
  const __kvDrainStream = async (stream) => {
    const limit = __kvLimits().maxValueBytes;
    const reader = stream.getReader();
    const chunks = [];
    let size = 0;
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        const chunk = ArrayBuffer.isView(value)
          ? new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
          : new Uint8Array(value);
        size += chunk.byteLength;
        if (size > limit) {
          throw __kvError(`a value is at most ${limit} bytes, and the stream is larger`);
        }
        chunks.push(chunk);
      }
    } catch (error) {
      await reader.cancel(error).catch(() => {});
      throw error;
    } finally {
      reader.releaseLock();
    }
    // The chunk wrappers can alias the buffers yielded by the stream. `set()`
    // copies their current bytes into one owned result, so later mutation of a
    // yielded buffer cannot change the stored value.
    const value = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      value.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return value;
  };

  const __kvDecodeValue = (bytes, tag, type) => {
    const view = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    switch (type) {
      case "arrayBuffer":
        return view.buffer.slice(view.byteOffset, view.byteOffset + view.byteLength);
      case "json":
        return JSON.parse(__kvDecoder.decode(view));
      case "stream":
        return new Response(view).body;
      case "text":
      default:
        // A value written as bytes still decodes as text when asked for text,
        // which is upstream's behaviour and the reason the tag is advisory
        // rather than a type check.
        return __kvDecoder.decode(view);
    }
  };

  const __kvReadOptions = (options) => {
    if (typeof options === "string") return { type: options };
    if (options === null || options === undefined) return { type: "text" };
    return { type: options.type ?? "text", cacheTtl: options.cacheTtl };
  };

  class KvNamespace {
    // `namespace()` answers the namespace of KV cells, and `cellName` names this
    // namespace's one cell (`celld_logic::kv::cell_name`).
    constructor(namespace, cellName) {
      Object.defineProperty(this, "_namespace", { value: namespace });
      Object.defineProperty(this, "_cellName", { value: cellName });
    }

    // Resolved per call rather than cached: a cell can move between calls, and
    // `getByName` costs what the Durable Object path already pays.
    get _stub() {
      return this._namespace().getByName(this._cellName);
    }

    async _read(keys, options, withMetadata) {
      const { type } = __kvReadOptions(options);
      for (const key of keys) __kvCheckKey(key);
      const rows = await this._stub.__kvGet({
        keys,
        withMetadata,
        now: Date.now(),
      });
      return rows.map((row) => {
        if (!row.found) return { key: row.key, value: null, metadata: null };
        return {
          key: row.key,
          value: __kvDecodeValue(row.value, row.tag, type),
          metadata: row.metadata === null || row.metadata === undefined
            ? null
            : JSON.parse(row.metadata),
        };
      });
    }

    // `get(key)` answers a value; `get([keys])` answers a Map with a null hole
    // for a key that is not there. Upstream's bulk form is a Map and not an
    // object, which matters: an object would collide a key named `__proto__`
    // with the prototype chain.
    async get(key, options) {
      if (Array.isArray(key)) {
        const rows = await this._read(__kvBulkKeys(key), options, false);
        return new Map(rows.map((row) => [row.key, row.value]));
      }
      const [row] = await this._read([String(key)], options, false);
      return row.value;
    }

    async getWithMetadata(key, options) {
      if (Array.isArray(key)) {
        const rows = await this._read(__kvBulkKeys(key), options, true);
        return new Map(
          rows.map((row) => [row.key, { value: row.value, metadata: row.metadata }]),
        );
      }
      const [row] = await this._read([String(key)], options, true);
      return {
        value: row.value,
        metadata: row.metadata,
        // Null, and honestly so: celld has no read cache, and reporting a HIT
        // from a runtime that never cached anything would be a lie in a field
        // applications read.
        cacheStatus: null,
      };
    }

    async put(key, value, options) {
      const name = String(key);
      __kvCheckKey(name);
      let encoded;
      if (typeof ReadableStream !== "undefined" && value instanceof ReadableStream) {
        encoded = { value: await __kvDrainStream(value), tag: __KV_TAG_BYTES };
      } else {
        encoded = __kvEncodeValue(value);
        __kvCheckValue(encoded.value.byteLength);
      }
      const metadata = options && options.metadata !== undefined
        ? JSON.stringify(options.metadata)
        : null;
      if (metadata !== null) __kvCheckMetadata(metadata);
      const expiresAt = __kvExpiryAt(
        Date.now(),
        options && options.expiration,
        options && options.expirationTtl,
      );
      await this._stub.__kvPut({
        entries: [{ key: name, value: encoded.value, tag: encoded.tag, metadata, expiresAt }],
      });
    }

    async delete(key) {
      const name = String(key);
      __kvCheckKey(name);
      await this._stub.__kvDelete({ keys: [name] });
    }

    // Upstream takes one key or an array, and caps the array at the same 100 a
    // bulk get takes.
    async deleteBulk(keys) {
      const names = Array.isArray(keys) ? __kvBulkKeys(keys) : [String(keys)];
      for (const name of names) __kvCheckKey(name);
      await this._stub.__kvDelete({ keys: names });
    }

    async list(options) {
      const prefix = options && options.prefix ? String(options.prefix) : "";
      const limit = __kvListLimit(options && options.limit);
      const after = options && options.cursor ? __kvCursor.decode(String(options.cursor)) : "";
      const page = await this._stub.__kvList({ prefix, limit, after, now: Date.now() });
      const keys = page.keys.map((entry) => {
        const key = { name: entry.name };
        if (entry.metadata !== null) key.metadata = JSON.parse(entry.metadata);
        // Upstream reports an expiration in seconds.
        if (entry.expiration !== null) key.expiration = Math.floor(entry.expiration / 1000);
        return key;
      });
      if (page.complete) return { keys, list_complete: true, cacheStatus: null };
      return {
        keys,
        list_complete: false,
        cursor: __kvCursor.encode(keys[keys.length - 1].name),
        cacheStatus: null,
      };
    }
  }

  // The bulk ceiling belongs to the binding, and a CLI chunks beneath it rather
  // than inheriting it. An empty array is refused because upstream refuses it:
  // answering an empty Map would look like "none of these keys exist".
  const __kvBulkKeys = (keys) => {
    if (keys.length === 0) throw __kvError("a bulk get needs at least one key");
    if (keys.length > __kvLimits().maxBulkKeys) {
      throw __kvError(
        `a bulk get takes at most ${__kvLimits().maxBulkKeys} keys, got ${keys.length}`,
      );
    }
    return keys.map(String);
  };

  return {
    KvNamespaceCell: __KvNamespaceCell,
    KvNamespace,
    // Queue retention runs the same bounded transaction.
    cellSweepBatch: __cellSweepBatch,
  };
}
