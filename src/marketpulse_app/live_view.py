"""Shopify-style Live View over the projected clean events; provisional, anchored at the latest event time."""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from typing import Any, Callable

from marketpulse_batch.publication import NoPublishedRelease

from .analytics import wire
from .live_events import TOPIC
from .math import ratio

WINDOW_MINUTES = 30
STAGES = ('browsing', 'viewing_product', 'cart', 'checkout', 'purchased')
DEVICES = ('desktop', 'mobile', 'tablet', 'unknown')
KINDS = {'page_view': 'browsing', 'view_item': 'viewed_product', 'add_to_cart': 'added_to_cart',
         'begin_checkout': 'started_checkout', 'purchase': 'purchased', 'order_created': 'order_placed',
         'order_refunded': 'order_refunded'}
UNLISTED = 'Unlisted product'
EMPTY = 'No live event has been projected yet; start the live events worker and the replay.'
WEB = "source IN ('web','mobile')"


class LiveViewService:
    def __init__(self, analytics: Any, database: str = 'marketpulse_live',
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)):
        self.analytics, self.database, self.clock = analytics, database, clock
        table = f'{database}.events'
        # Same admission as Silver: Flink-eligible and consented; bot sessions drop web/app events only, never orders.
        self.events = (f"(SELECT * FROM {table} FINAL WHERE eligible_for_aggregates AND consent_analytics "
                       f"AND NOT (source IN ('web','mobile') AND (is_bot OR ifNull(session_id,'') IN (SELECT assumeNotNull(session_id) "
                       f"FROM {table} FINAL WHERE is_bot AND session_id IS NOT NULL))))")

    def _query(self, sql: str, parameters: dict[str, object] | None = None) -> list[dict[str, object]]:
        return self.analytics._query(sql, parameters)

    def view(self) -> dict[str, object]:
        present = self._query("SELECT count() AS present FROM system.tables WHERE database={database:String} AND name='events'",
                              {'database': self.database})[0]['present']
        if not int(present):
            return wire({'status': 'empty', 'message': EMPTY})
        head = self._query(f'SELECT max(event_time) AS anchor, count() AS eligible FROM {self.events}')[0]
        if not int(head['eligible']):
            return wire({'status': 'empty', 'message': EMPTY})
        anchor = head['anchor'].astimezone(timezone.utc)
        window = {'anchor': anchor, 'start': anchor - timedelta(minutes=WINDOW_MINUTES)}
        day = {'anchor': anchor, 'day_start': datetime.combine(anchor.date(), time.min, timezone.utc)}
        products, activity = self._products(day), self._activity(anchor)
        labels = self._labels({row['product_id'] for row in products} | {row['product'] for row in activity if row['product']})
        return wire({'status': 'available', 'anchor': anchor, 'window_minutes': WINDOW_MINUTES,
                     'active': self._active(window), 'behaviour': self._behaviour(window), 'today': self._today(day),
                     'series': self._series(anchor),
                     'top_products': [{'label': labels.get(row['product_id'], (UNLISTED, None))[0],
                                       'category': labels.get(row['product_id'], (UNLISTED, None))[1],
                                       'units': int(row['units']), 'carts': int(row['carts']), 'views': int(row['views'])} for row in products],
                     'activity': [{key: value for key, value in row.items() if key != 'product'}
                                  | {'label': labels.get(row['product'], (UNLISTED, None))[0] if row['product'] else None} for row in activity],
                     **self._freshness_and_provenance(anchor)})

    def _active(self, window: dict[str, object]) -> dict[str, object]:
        where = f"{WEB} AND event_time > {{start:DateTime64(6,'UTC')}} AND event_time <= {{anchor:DateTime64(6,'UTC')}}"
        total = self._query(f'SELECT uniqExact(identity_key) AS active_visitors FROM {self.events} WHERE {where}', window)[0]
        sources = self._query(f'SELECT source, uniqExact(identity_key) AS visitors FROM {self.events} WHERE {where} GROUP BY source', window)
        devices = {row['device_type']: int(row['visitors']) for row in self._query(
            f'SELECT device_type, uniqExact(identity_key) AS visitors FROM {self.events} WHERE {where} GROUP BY device_type', window)}
        return {'visitors': int(total['active_visitors']),
                'by_source': sorted(({'source': row['source'], 'visitors': int(row['visitors'])} for row in sources), key=lambda r: r['source']),
                'by_device': [{'device': device, 'visitors': devices.get(device, 0)} for device in DEVICES]}

    def _behaviour(self, window: dict[str, object]) -> list[dict[str, object]]:
        rows = self._query(f"""SELECT stage, count() AS sessions FROM (
            SELECT session_id, max(multiIf(event_name='purchase',5,event_name='begin_checkout',4,event_name='add_to_cart',3,
                                           event_name='view_item',2,1)) AS stage
            FROM {self.events} WHERE {WEB} AND event_time <= {{anchor:DateTime64(6,'UTC')}} AND session_id IN (
                SELECT session_id FROM {self.events} WHERE {WEB} AND event_time > {{start:DateTime64(6,'UTC')}}
                AND event_time <= {{anchor:DateTime64(6,'UTC')}})
            GROUP BY session_id) GROUP BY stage""", window)
        counts = {int(row['stage']): int(row['sessions']) for row in rows}
        return [{'stage': stage, 'sessions': counts.get(index, 0)} for index, stage in enumerate(STAGES, start=1)]

    def _today(self, day: dict[str, object]) -> dict[str, object]:
        where = "event_time >= {day_start:DateTime64(6,'UTC')} AND event_time <= {anchor:DateTime64(6,'UTC')}"
        people = self._query(f"""SELECT uniqExact(identity_key) AS visitors, uniqExact(session_id) AS sessions,
            uniqExactIf(session_id, event_name='purchase') AS purchased FROM {self.events} WHERE {WEB} AND {where}""", day)[0]
        money = self._query(f"""SELECT currency, countIf(event_name='order_created') AS orders,
            sumIf(toInt128(gross_amount_minor), event_name='order_created') AS gross,
            sumIf(toInt128(refund_amount_minor), event_name='order_refunded') AS refunds
            FROM {self.events} WHERE source='commerce' AND {where} GROUP BY currency ORDER BY currency""", day)
        sessions, purchased = int(people['sessions']), int(people['purchased'])
        return {'visitors': int(people['visitors']), 'sessions': sessions, 'purchased_sessions': purchased,
                'conversion': ratio(purchased, sessions),
                'money': [{'currency': row['currency'], 'orders': int(row['orders']), 'gross_minor': int(row['gross']),
                           'refunds_minor': int(row['refunds']), 'net_minor': int(row['gross']) - int(row['refunds'])} for row in money]}

    def _series(self, anchor: datetime) -> list[dict[str, object]]:
        last = anchor.replace(second=0, microsecond=0)
        first = last - timedelta(minutes=WINDOW_MINUTES - 1)
        rows = self._query(f"""SELECT toStartOfMinute(event_time) AS minute, uniqExactIf(identity_key, {WEB}) AS visitors,
            countIf(event_name='order_created') AS orders FROM {self.events}
            WHERE event_time >= {{start:DateTime64(6,'UTC')}} AND event_time <= {{anchor:DateTime64(6,'UTC')}} GROUP BY minute""",
            {'start': first, 'anchor': anchor})
        values = {row['minute'].astimezone(timezone.utc).replace(second=0, microsecond=0): row for row in rows}
        minutes = [first + timedelta(minutes=step) for step in range(WINDOW_MINUTES)]
        return [{'minute': minute, 'visitors': int(values[minute]['visitors']) if minute in values else 0,
                 'orders': int(values[minute]['orders']) if minute in values else 0} for minute in minutes]

    def _products(self, day: dict[str, object]) -> list[dict[str, object]]:
        where = "event_time >= {day_start:DateTime64(6,'UTC')} AND event_time <= {anchor:DateTime64(6,'UTC')}"
        return self._query(f"""SELECT product_id, sum(units) AS units, sum(carts) AS carts, sum(views) AS views FROM (
            SELECT assumeNotNull(product_id) AS product_id, toUInt64(0) AS units, toUInt64(event_name='add_to_cart') AS carts,
                   toUInt64(event_name='view_item') AS views
            FROM {self.events} WHERE event_name IN ('view_item','add_to_cart') AND product_id IS NOT NULL AND {where}
            UNION ALL
            SELECT tupleElement(item,1) AS product_id, toUInt64(tupleElement(item,2)) AS units, toUInt64(0) AS carts, toUInt64(0) AS views
            FROM {self.events} ARRAY JOIN items AS item WHERE event_name='order_created' AND {where})
            GROUP BY product_id ORDER BY units DESC, carts DESC, views DESC, product_id LIMIT 5""", day)

    def _activity(self, anchor: datetime) -> list[dict[str, object]]:
        rows = self._query(f"""SELECT event_time, event_name, source, device_type, product_id, items, currency,
            gross_amount_minor, refund_amount_minor FROM {self.events}
            WHERE event_name IN ({','.join(f"'{name}'" for name in KINDS)}) AND event_time <= {{anchor:DateTime64(6,'UTC')}}
            ORDER BY event_time DESC, partition DESC, offset DESC LIMIT 12""", {'anchor': anchor})
        result = []
        for row in rows:
            name = row['event_name']
            amount = {'order_created': row['gross_amount_minor'], 'order_refunded': row['refund_amount_minor']}.get(name)
            items = row['items'] or []
            result.append({'event_time': row['event_time'], 'kind': KINDS[name], 'source': row['source'], 'device': row['device_type'],
                           'product': row['product_id'] or (items[0][0] if items else None),
                           'amount_minor': int(amount) if amount is not None else None,
                           'currency': row['currency'] if amount is not None else None})
        return result

    def _labels(self, product_ids: set[str]) -> dict[str, tuple[str, str | None]]:
        if not product_ids:
            return {}
        try:
            database = self.analytics.pin().release.database
        except NoPublishedRelease:
            return {}
        present = self._query("SELECT count() AS present FROM system.tables WHERE database={database:String} AND name='commerce_daily'",
                              {'database': database})[0]['present']
        if not int(present):
            return {}
        rows = self._query(f"""SELECT product_id, any(product_name) AS name, any(category) AS category FROM {database}.commerce_daily
            WHERE product_id IN {{ids:Array(String)}} GROUP BY product_id""", {'ids': sorted(product_ids)})
        return {row['product_id']: (row['name'], row['category']) for row in rows}

    def _freshness_and_provenance(self, anchor: datetime) -> dict[str, object]:
        table = f'{self.database}.events'
        stored = self._query(f'SELECT count() AS rows, max(received_at) AS last_received FROM {table} FINAL')[0]
        last = stored['last_received'].astimezone(timezone.utc)
        seconds = max(0, int((self.clock() - last).total_seconds()))
        identity = self._query(f'SELECT DISTINCT identity FROM {self.database}.event_source_identity')
        bounds = self._query(f"""SELECT partition, min(offset) AS from_offset, max(offset)+1 AS to_offset FROM {table} FINAL
            GROUP BY partition ORDER BY partition""")
        return {'freshness': {'anchor': anchor, 'last_received_at': last, 'seconds_since_receipt': seconds, 'receiving': seconds <= 60},
                'provenance': {'source': TOPIC, 'source_identity': identity[0]['identity'] if len(identity) == 1 else None,
                               'rows': int(stored['rows']),
                               'offset_bounds': [{'partition': int(row['partition']), 'from_offset': int(row['from_offset']),
                                                  'to_offset': int(row['to_offset'])} for row in bounds],
                               'note': 'Provisional · not certified'}}
