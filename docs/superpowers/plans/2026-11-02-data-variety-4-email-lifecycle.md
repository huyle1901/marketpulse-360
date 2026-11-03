# Data Variety 4 — Email Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fixed, decaying story email list with a growing lifecycle programme of welcome, newsletter and promo emails that send shoppers back to the site.

**Architecture:** A per-world email schedule computed once (audience by join date, one send per customer per day, unsubscribe day per customer) drives daily email events; each click spawns an email-referred session using plan 3's non-paid path.

**Tech Stack:** Python 3.12, JSON Schema contracts.

**Spec:** `docs/superpowers/specs/2026-11-02-data-variety-4-email-lifecycle-design.md`. Builds on plan 1 (join dates) and plan 3 (`is_promo_day`, `traffic_source`, non-paid sessions).

## Global Constraints

- Audience: marketing-consented customers from `created_at` until unsubscribed; no new opt-in/opt-out behaviour.
- Types and rates (open / click of delivered): welcome 55 % / 12 %, newsletter 28 % / 2.5 %, promo 32 % / 4.5 %; unsubscribe 0.5 % per delivered email.
- Schedule: welcome on join day; newsletter on Vietnam Tuesdays; promo on `is_promo_day`; at most one email per customer per day, priority welcome > promo > newsletter.
- Email-referred session within 0–2 h of the click, `traffic_source = email`, no click id, strength ×1.2.
- `flat` keeps today's email behaviour byte for byte.

## Carried from plan 3 (final review)

- Non-paid web events (organic, direct, email) carry no campaign or click id; only the order credits a paid click
  (`_non_paid_purchase_touch`, 7-day window, bot sessions excluded). Email orders follow the same rule.
- Email-referred sessions are a third session kind; see Task 3's Interfaces for timing and id rules.

## Review Focus

- A customer who unsubscribes from a welcome email receives no newsletter later that week — test in Task 1.
- 11/11/2025 is a Tuesday and a double day → exactly one promo, no newsletter — test in Task 1.
- Email events and email-referred sessions stay inside their UTC day: sends go out 08:00–10:00 Vietnam time, opens follow within 8 h, clicks within 30 min of the open and sessions within 2 h of the click, so every email session starts before 16:00 UTC of its click's day and no carry-in across midnight is needed — test in Task 3.
- A customer whose CRM consent is false never enters the audience — test in Task 1.
- Generation time stays linear in days (the old unsubscribe check was quadratic) — test in Task 1 (365-day schedule builds in under 5 s).

---

### Task 1: Email schedule and audience

**Files:** Create `src/marketpulse_mock/email_schedule.py`; Modify `src/marketpulse_mock/journeys.py` (`_iter_email_events` uses it for `vietnam-retail`); Test `tests/test_email_schedule.py`.

**Interfaces:** `@dataclass(frozen=True) class EmailSend: customer: Customer; email_type: Literal['welcome','newsletter','promo']`; `EmailSchedule.build(config, masters) -> EmailSchedule` (computes each customer's unsubscribe date by walking their sends once); `EmailSchedule.sends_on(day: date) -> tuple[EmailSend, ...]`; `EmailSchedule.unsubscribed_on(customer_id) -> date | None`. Cached per `(seed, profile.name, days)`.

- [ ] **Step 1: Failing tests:** sends in the last 30 days > sends in the first 30 (story, full window); each in-window consented joiner has exactly one welcome on their join date; no send before `created_at` or after `unsubscribed_on`; on 2025-11-11 types are promo/welcome only; newsletters only on Vietnam Tuesdays; non-consented customers never appear; a 365-day schedule builds in < 5 s.
- [ ] **Step 2: Run** `PYTHONPATH=src .venv/bin/python -m unittest tests.test_email_schedule -v` — Expected: FAIL.
- [ ] **Step 3: Implement**; open/click/unsubscribe outcomes from `stable_int(seed, "email-open"|"email-click"|"email-unsub", day, customer)`.
- [ ] **Step 4: Run** the new tests and `tests.test_journeys` (`test_unsubscribed_customers_receive_no_later_email_sends` must still pass) — Expected: PASS.
- [ ] **Step 5: Commit** `feat: grow the story email audience with a lifecycle schedule`.

### Task 2: Email types on events and contract

**Files:** Modify `src/marketpulse_mock/journeys.py`, `contracts/kafka/email-events-v1.json` (optional `email_type` enum in payload); Test `tests/test_journeys_variety.py`, `tests/test_kafka_contracts.py`.

- [ ] **Step 1: Failing tests:** over 90 days each type's open and click rates are within ±5 points of the targets; every story email event carries `email_type`; a fixture email event (no `email_type`) still validates; `email_type: "survey"` fails the contract.
- [ ] **Step 2: Run** — Expected: FAIL.
- [ ] **Step 3: Implement**; `lifecycle_campaign_id` = `LC-<type>-<yyyymmdd>`.
- [ ] **Step 4: Run** Python tests — Expected: PASS.
- [ ] **Step 5: Commit** `feat: label story emails by lifecycle type`.

### Task 3: Email-referred visits

**Files:** Modify `src/marketpulse_mock/journeys.py`; Test `tests/test_journeys_variety.py`.

**Interfaces:** Consumes plan 3's `emit_session(ordinal, None, event_time, "email", strength)` with strength 1.2 (×1.0 base, not ×`NON_PAID_STRENGTH`; device and promo multipliers still apply), extended with the recipient as the session's customer (not `picker.pick`). The session time = click time + `stable_int(..., modulo=7200)` seconds; the send window keeps it inside the click's UTC day (see Review Focus). Email sessions get their own id scope (e.g. `-email-<day>`) and ordinal range above the non-paid range, and the day's session list carries an explicit kind per entry (`paid`, `visit`, `email`) instead of dispatching on ordinal ranges.

- [ ] **Step 1: Failing tests:** every `traffic_source = email` session has an `email_clicked` for the same customer 0–2 h earlier on the same UTC day; none has a campaign or click id on any web event; on promo days email sessions are 10–35 % of all sessions; every record lies in its own UTC day and the replay reader finds no event-time violation; generating day N alone equals day N from a sequential run; email session ids never collide with organic/direct ids.
- [ ] **Step 2: Run** — Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** all Python tests — Expected: PASS, fixture unchanged.
- [ ] **Step 5: Commit** `feat: send email clickers back to the store`; open the PR.
