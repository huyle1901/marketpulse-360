# Real-time replay publication

## Problem

Live View received events in bursts minutes apart. The resumable replay paced every record of
a batch first and only then published the whole batch, so a record was held until the last
record of its batch came due. With `--batch-size 20` at `--speed 20` and a quiet night-time
store, a batch took about six real minutes to fill: ClickHouse saw 20 events at 03:09 and the
next 19 at 03:15, while Flink (5 s checkpoints), the worker and the 5 s UI poll add seconds.

## Decision

`replay_window` publishes each record as soon as the pacer says it is due. The batch stays the
unit of acknowledgement and cursor commit:

- the pending marker is written immediately before a batch's first send;
- after the batch (or a stop inside it) the publisher is flushed, every sent record must be
  acknowledged, and the cursor advances by exactly the records sent;
- a stop between records therefore leaves no pending range and resumes after the last sent
  record; a crash after a send still leaves the pending range and fails closed as before.

Event-time order, pacing (`--speed`, `--events-per-second`) and the cursor file format are
unchanged.

## Tests

`tests/test_replay_cursor.py`: records alternate wait → publish; a stop mid-batch commits the
records already sent and a resume continues without repeating or skipping any event.
