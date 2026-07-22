# High-Concurrency PostgreSQL Transaction Isolation and Lock Mitigation

This guide documents safe transaction patterns for WorkSphere when PostgreSQL handles concurrent bookings, collections, ratings, notifications, and other write-heavy workflows.

## 1. Goals

Concurrent requests can produce lost updates, duplicate reservations, inconsistent counters, lock waits, deadlocks, serialization failures, and exhausted pools. Use the least expensive isolation level that still preserves the business invariant.

## 2. PostgreSQL isolation levels

| Level             | Behavior                                                       | Use in WorkSphere                                      |
| ----------------- | -------------------------------------------------------------- | ------------------------------------------------------ |
| `READ COMMITTED`  | Each statement sees data committed before that statement began | Default CRUD and independent writes                    |
| `REPEATABLE READ` | All statements use one stable transaction snapshot             | Multi-step reports and consistent calculations         |
| `SERIALIZABLE`    | PostgreSQL rejects executions that cannot be ordered serially  | Last-seat allocation, booking conflicts, strict quotas |

PostgreSQL treats `READ UNCOMMITTED` as `READ COMMITTED`.

### Read Committed

Use the default for independent operations and rely on constraints plus atomic Prisma operations:

```ts
await prisma.venueRating.upsert({
  where: { userId_venueId: { userId, venueId } },
  update: ratingData,
  create: { userId, venueId, ...ratingData },
});
```

### Repeatable Read

```ts
import { Prisma } from "@prisma/client";

const snapshot = await prisma.$transaction(
  async (tx) => {
    const venue = await tx.venue.findUniqueOrThrow({ where: { id: venueId } });
    const ratings = await tx.venueRating.findMany({ where: { venueId } });
    return { venue, ratings };
  },
  {
    isolationLevel: Prisma.TransactionIsolationLevel.RepeatableRead,
    maxWait: 2_000,
    timeout: 5_000,
  },
);
```

### Serializable

Use for strict invariants and retry the complete transaction when PostgreSQL reports a serialization failure.

```ts
const booking = await prisma.$transaction(
  async (tx) => {
    const conflict = await tx.booking.findFirst({
      where: {
        venueId,
        startsAt: { lt: requestedEnd },
        endsAt: { gt: requestedStart },
        status: { in: ["PENDING", "CONFIRMED"] },
      },
      select: { id: true },
    });

    if (conflict) throw new Error("BOOKING_CONFLICT");

    return tx.booking.create({ data: bookingData });
  },
  {
    isolationLevel: Prisma.TransactionIsolationLevel.Serializable,
    maxWait: 2_000,
    timeout: 5_000,
  },
);
```

Never perform external API calls, email delivery, AI requests, file processing, or user interaction inside an interactive transaction.

## 3. Isolation-level decision table

| Invariant                              | Recommended technique               |
| -------------------------------------- | ----------------------------------- |
| One rating/favorite per user and venue | Unique constraint plus `upsert`     |
| Independent notification inserts       | `createMany` or `$transaction([])`  |
| Stable report snapshot                 | `REPEATABLE READ`                   |
| Last available seat                    | `SERIALIZABLE` plus retry           |
| Simple counter                         | Atomic `{ increment: 1 }` update    |
| Multi-row read-modify-write            | `SERIALIZABLE` or explicit row lock |

## 4. Prisma batching patterns

### Nested writes

```ts
await prisma.conversation.create({
  data: {
    userId,
    title,
    messages: { create: initialMessages },
  },
});
```

### Bulk operations

```ts
await prisma.notification.createMany({
  data: recipients.map((userId) => ({ userId, title, body })),
  skipDuplicates: true,
});
```

Prefer this over a loop of individual inserts.

### Sequential transaction batch

```ts
const [booking, auditLog] = await prisma.$transaction([
  prisma.booking.create({ data: bookingData }),
  prisma.auditLog.create({ data: auditData }),
]);
```

### Interactive transaction

Use only when later queries depend on earlier results.

```ts
await prisma.$transaction(
  async (tx) => {
    const folder = await tx.folder.findUniqueOrThrow({
      where: { id: folderId },
      select: { ownerId: true },
    });

    if (folder.ownerId !== userId) throw new Error("FORBIDDEN");

    await tx.folderMember.upsert({
      where: { folderId_userId: { folderId, userId: memberId } },
      update: { role },
      create: { folderId, userId: memberId, role },
    });
  },
  { maxWait: 2_000, timeout: 5_000 },
);
```

## 5. Retry transient concurrency failures

Important PostgreSQL codes:

| Code    | Meaning                |
| ------- | ---------------------- |
| `40001` | Serialization failure  |
| `40P01` | Deadlock detected      |
| `55P03` | Lock not available     |
| `57014` | Query canceled/timeout |

```ts
type RetryOptions = {
  attempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function isRetryableTransactionError(error: unknown) {
  if (!(error instanceof Error)) return false;

  const candidate = error as Error & {
    code?: string;
    meta?: { code?: string; database_error?: string };
  };

  const details = [
    candidate.code,
    candidate.meta?.code,
    candidate.meta?.database_error,
    candidate.message,
  ]
    .filter(Boolean)
    .join(" ");

  return /40001|40P01|55P03|serialization|deadlock/i.test(details);
}

export async function withTransactionRetry<T>(
  operation: () => Promise<T>,
  { attempts = 3, baseDelayMs = 50, maxDelayMs = 500 }: RetryOptions = {},
): Promise<T> {
  let lastError: unknown;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (!isRetryableTransactionError(error) || attempt === attempts) {
        throw error;
      }

      const delay = Math.min(baseDelayMs * 2 ** (attempt - 1), maxDelayMs);
      const jitter = Math.floor(Math.random() * baseDelayMs);
      await sleep(delay + jitter);
    }
  }

  throw lastError;
}
```

Keep retries bounded; excessive retries amplify overload.

## 6. Deadlock prevention

1. Lock or update rows in a consistent order.
2. Keep transactions short.
3. Never wait for user input inside a transaction.
4. Index predicates used by contended updates.
5. Use atomic updates instead of read-modify-write.
6. Prefer unique/check/exclusion constraints over application-only checks.
7. Batch writes and avoid unbounded `Promise.all` database calls.

Consistent ordering example:

```ts
const orderedIds = [...venueIds].sort();

await prisma.$transaction(async (tx) => {
  for (const id of orderedIds) {
    await tx.venue.update({ where: { id }, data: { updatedAt: new Date() } });
  }
});
```

Atomic counter:

```ts
await prisma.venue.update({
  where: { id: venueId },
  data: { favoriteCount: { increment: 1 } },
});
```

## 7. Explicit row locking

Prisma does not expose every PostgreSQL lock clause through its standard API. Use parameterized raw SQL when strict row locking is necessary.

```ts
type CapacityRow = { id: string; capacity: number; occupied: number };

await prisma.$transaction(async (tx) => {
  const rows = await tx.$queryRaw<CapacityRow[]>`
    SELECT id, capacity, occupied
    FROM "VenueSeatPool"
    WHERE id = ${poolId}
    FOR UPDATE
  `;

  const pool = rows[0];
  if (!pool || pool.occupied >= pool.capacity) throw new Error("NO_CAPACITY");

  await tx.venueSeatPool.update({
    where: { id: poolId },
    data: { occupied: { increment: 1 } },
  });
});
```

Use tagged `$queryRaw`, lock only required rows, and keep a deterministic order.

## 8. Deadlock and lock analysis SQL

### Active sessions and waits

```sql
SELECT
  pid,
  usename,
  application_name,
  client_addr,
  state,
  wait_event_type,
  wait_event,
  now() - query_start AS query_age,
  now() - xact_start AS transaction_age,
  left(query, 500) AS query
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid()
ORDER BY transaction_age DESC NULLS LAST;
```

### Waiting sessions and blockers

```sql
SELECT
  waiting.pid AS waiting_pid,
  waiting.usename AS waiting_user,
  now() - waiting.query_start AS waiting_duration,
  left(waiting.query, 300) AS waiting_query,
  blocking.pid AS blocking_pid,
  blocking.usename AS blocking_user,
  now() - blocking.query_start AS blocking_duration,
  left(blocking.query, 300) AS blocking_query
FROM pg_stat_activity AS waiting
CROSS JOIN LATERAL unnest(pg_blocking_pids(waiting.pid)) AS blocker(blocking_pid)
JOIN pg_stat_activity AS blocking
  ON blocking.pid = blocker.blocking_pid
ORDER BY waiting_duration DESC;
```

### Lock inventory

```sql
SELECT
  l.pid,
  a.usename,
  a.state,
  l.locktype,
  l.mode,
  l.granted,
  l.relation::regclass AS relation,
  l.virtualxid,
  l.transactionid,
  now() - a.query_start AS query_age,
  left(a.query, 300) AS query
FROM pg_locks AS l
LEFT JOIN pg_stat_activity AS a ON a.pid = l.pid
WHERE a.datname = current_database()
ORDER BY l.granted, query_age DESC NULLS LAST;
```

### Long-running and idle transactions

```sql
SELECT
  pid,
  usename,
  state,
  now() - xact_start AS transaction_age,
  now() - state_change AS state_age,
  wait_event_type,
  wait_event,
  left(query, 500) AS query
FROM pg_stat_activity
WHERE xact_start IS NOT NULL
  AND pid <> pg_backend_pid()
ORDER BY transaction_age DESC;
```

Cancel a query:

```sql
SELECT pg_cancel_backend(<pid>);
```

Terminate a session only after confirming the blocker:

```sql
SELECT pg_terminate_backend(<pid>);
```

## 9. Timeout rules

Recommended starting points for normal APIs:

| Setting                               | Initial value | Purpose                                  |
| ------------------------------------- | ------------: | ---------------------------------------- |
| Prisma `maxWait`                      |     2 seconds | Maximum wait to acquire a transaction    |
| Prisma `timeout`                      |     5 seconds | Maximum interactive transaction duration |
| PostgreSQL `lock_timeout`             |   1–3 seconds | Maximum lock wait                        |
| PostgreSQL `statement_timeout`        |  5–15 seconds | Maximum statement execution              |
| `idle_in_transaction_session_timeout` | 15–30 seconds | Ends abandoned open transactions         |
| Connection `connect_timeout`          |  5–10 seconds | Limits connection establishment          |

Per-transaction example:

```ts
await prisma.$transaction(
  async (tx) => {
    await tx.$executeRaw`SET LOCAL lock_timeout = '2s'`;
    await tx.$executeRaw`SET LOCAL statement_timeout = '5s'`;
    await tx.booking.create({ data: bookingData });
  },
  { maxWait: 2_000, timeout: 6_000 },
);
```

Keep Prisma's transaction timeout slightly above the database statement timeout.

## 10. Neon pool tuning

Use the pooled Neon URL for runtime traffic and a direct URL for migrations and administrative work.

```env
DATABASE_URL="postgresql://USER:PASSWORD@ep-example-pooler.region.aws.neon.tech/DB?sslmode=require"
DIRECT_URL="postgresql://USER:PASSWORD@ep-example.region.aws.neon.tech/DB?sslmode=require"
```

Prisma 7 CLI configuration:

```ts
import "dotenv/config";
import { defineConfig, env } from "prisma/config";

export default defineConfig({
  schema: "prisma/schema.prisma",
  migrations: { path: "prisma/migrations" },
  datasource: { url: env("DIRECT_URL") },
});
```

Operational rules:

- Reuse one Prisma Client per long-running process.
- Use the pooled endpoint for API/serverless traffic.
- Use direct connections for migrations and session-dependent tools.
- Keep transactions short because they hold a backend connection.
- Bound concurrency for bulk jobs.
- Batch writes.
- Monitor pool wait time, active connections, and idle transactions.

Inspect capacity:

```sql
SHOW max_connections;
```

```sql
SELECT
  count(*) AS current_connections,
  current_setting('max_connections')::int AS max_connections,
  round(
    100.0 * count(*) / current_setting('max_connections')::int,
    2
  ) AS utilization_percent
FROM pg_stat_activity;
```

Connections by state:

```sql
SELECT state, count(*) AS connections
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY state
ORDER BY connections DESC;
```

Pool exhaustion symptoms include rising request latency, Prisma transaction acquisition timeouts, queued requests, and many long or idle transactions. Do not increase connection counts before removing long transactions, N+1 queries, and unbounded parallelism.

## 11. Bounded bulk concurrency

Avoid thousands of simultaneous database promises.

Prefer:

```ts
await prisma.venue.updateMany({
  where: { id: { in: venueIds } },
  data: update,
});
```

For distinct per-item operations, process small batches:

```ts
const concurrency = 5;

for (let index = 0; index < tasks.length; index += concurrency) {
  const batch = tasks.slice(index, index + concurrency);
  await Promise.all(batch.map(processTask));
}
```

## 12. Observability

Track:

- transaction duration;
- acquisition wait;
- retry count and success rate;
- deadlock and serialization-failure counts;
- statement and lock timeouts;
- active and idle-in-transaction sessions;
- pool wait duration;
- connection utilization.

Never log connection strings, passwords, authentication tokens, or sensitive personal data.

## 13. Concurrency testing

Concurrency safety requires integration tests using PostgreSQL.

```ts
const attempts = Array.from({ length: 20 }, (_, index) =>
  createSeatReservation({
    userId: `test-user-${index}`,
    venueId,
    seatId,
  }),
);

const results = await Promise.allSettled(attempts);
const successful = results.filter((result) => result.status === "fulfilled");
expect(successful).toHaveLength(1);
```

Also test retry exhaustion, idempotency, lock timeouts, and bounded pool behavior. Never run destructive load tests against production.

## 14. Review checklist

- [ ] The transaction protects a documented invariant.
- [ ] The isolation level is justified.
- [ ] Constraints back up application checks.
- [ ] Transactions contain no external network calls.
- [ ] Lock/update order is deterministic.
- [ ] `maxWait` and `timeout` are bounded.
- [ ] Retry logic covers serialization failures and deadlocks where needed.
- [ ] Bulk APIs replace per-row loops where possible.
- [ ] Atomic updates replace read-modify-write counters.
- [ ] Lock predicates are indexed.
- [ ] Raw SQL is parameterized.
- [ ] Runtime uses a pooled Neon URL.
- [ ] Migrations use a direct URL.
- [ ] Prisma Client is reused.
- [ ] Bulk-job concurrency is bounded.
- [ ] Deadlock diagnostics are documented.
- [ ] Timeout errors map to safe API responses.

## 15. Recommended WorkSphere defaults

Ordinary API transactions:

```ts
{
  maxWait: 2_000,
  timeout: 5_000,
  isolationLevel: Prisma.TransactionIsolationLevel.ReadCommitted,
}
```

Strict booking/capacity operations:

```ts
{
  maxWait: 2_000,
  timeout: 5_000,
  isolationLevel: Prisma.TransactionIsolationLevel.Serializable,
}
```

Pair serializable operations with at most three retry attempts, exponential backoff with jitter, database constraints, and structured retry metrics.
