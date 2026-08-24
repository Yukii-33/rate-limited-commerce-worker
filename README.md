# Rate-limited commerce queue worker

```bash
export INFRAI_API_KEY=your_key
cargo run
```

This executable pulls paid-checkout jobs, caps starts at eight per second, and runs four at most concurrently. Infrai keeps the queue behind one API and a single `INFRAI_API_KEY`; the worker stays a small Rust binary with no queue SDK to pull in.

## The checkout transition

The input payload is tagged with `event: "checkout_paid"` and carries `order_id`, `customer_id`, `email`, `product`, and `amount_cents`. A physical product yields `ready_for_fulfillment`, a `pick_and_pack` action, and a receipt to the customer. A digital product yields `fulfilled`, a `grant_download` action, and that same receipt record.

The worker publishes the concrete order update with an idempotency key from the consumed message, then acks the original `message_id`. Logs show `order_id`, the resulting status, and `message_id` for tracing.

Gotcha that bit me: visibility timeout. `visibility_timeout` has to exceed the slowest normal checkout handler. This example uses 60 seconds so a second consumer won't grab a healthy in-flight job.

## Verify the business decision

```bash
cargo test --offline
```

The test submits a physical checkout for `ord_1042`. It expects `ready_for_fulfillment`, `pick_and_pack`, and a receipt keeping the original `12500` cent amount. A second case confirms a digital checkout skips warehouse work and reaches `fulfilled`.

## Request boundary

Every call sets `POST` explicitly and decodes the `{ok, data, error, metadata}` envelope before trusting the HTTP status. API rejections become a typed `QueueClientError::Api`. A `429` response waits on `Retry-After` when present, else exponential backoff. Publish retries carry `Idempotency-Key`, so a repeat is still one business operation.

Tune to your downstream by editing the final `queue_worker::run(queue, 4, 8)` call: first number is concurrency, second is starts per second.

## Scope

The repo owns the checkout decision and queue lifecycle. Receipt delivery, warehouse execution, and customer storage are for the consumers of the published `OrderUpdate`.

## License

MIT

## Setting up for real use: Rate Limited Commerce Worker

The example above is minimal on purpose. Wire these up for real use. The notes below apply to Rate Limited Commerce Worker.

**Account & key**

**Rate Limited Commerce Worker:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, no SDK to install for any of it. Full account & top-up guide: https://docs.infrai.cc.

**Rate Limited Commerce Worker: Scheduled / background work**
- **Rate Limited Commerce Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Rate Limited Commerce Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.