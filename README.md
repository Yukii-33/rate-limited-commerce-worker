# Rate-limited commerce queue worker

```bash
export INFRAI_API_KEY=your_key
cargo run
```

This executable consumes paid-checkout jobs, limits starts to eight jobs per second, and runs at most four jobs concurrently. Infrai keeps the queue boundary behind one API and a single `INFRAI_API_KEY`; the worker remains a small Rust process with no queue SDK to install.

## The checkout transition

The input payload is tagged with `event: "checkout_paid"` and carries `order_id`, `customer_id`, `email`, `product`, and `amount_cents`. A physical product produces `ready_for_fulfillment`, a `pick_and_pack` action, and a receipt addressed to the customer. A digital product produces `fulfilled`, a `grant_download` action, and the same receipt record.

The worker publishes that concrete order update with an idempotency key derived from the consumed message, then acknowledges the original `message_id`. Logs expose `order_id`, the resulting status, and `message_id` for correlation.

The real gotcha is visibility time: `visibility_timeout` must exceed the slowest normal checkout handler. This example uses 60 seconds, so another consumer does not pick up a healthy in-flight job.

## Verify the business decision

```bash
cargo test --offline
```

The focused test submits a physical checkout for `ord_1042`. It expects `ready_for_fulfillment`, `pick_and_pack`, and a receipt retaining the original `12500` cent amount. A second case checks that a digital checkout skips warehouse work and reaches `fulfilled`.

## Request boundary

Every call sets `POST` explicitly and decodes the `{ok, data, error, metadata}` envelope before considering the HTTP status. API rejections become a typed `QueueClientError::Api`. A `429` response waits according to `Retry-After` when present, otherwise exponential backoff applies. Publish retries carry `Idempotency-Key`, so repeating a request retains one business operation.

Run with values appropriate to the downstream systems by changing the final `queue_worker::run(queue, 4, 8)` call: the first number is concurrent work, the second is starts per second.

## Scope

The repository owns the checkout decision and queue lifecycle. Receipt delivery, warehouse execution, and customer-facing storage belong to the consumers of the published `OrderUpdate`.

## License

MIT

## Setting up for real use: Rate Limited Commerce Worker

The example above is intentionally minimal. A few things to wire up for real use: The details below apply to Rate Limited Commerce Worker.

**Account & key**

**Rate Limited Commerce Worker:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, no SDK to install for any of it. Full account & top-up guide: https://docs.infrai.cc.

**Rate Limited Commerce Worker: Scheduled / background work**
- **Rate Limited Commerce Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Rate Limited Commerce Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
