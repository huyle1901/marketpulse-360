"""Serving-only marketing insights over certified relations and the pinned journey projection."""
from __future__ import annotations

from .analytics import wire
from .lifecycle import LifecycleError, LifecycleService
from .math import ratio

TIERS = ('gold', 'silver', 'bronze', 'none')
UNAVAILABLE = 'Publish the pinned journey detail for this certified release first.'


def band_width(p95: int) -> int:
    """Smallest 1/2/5 x 10^k width that spreads orders up to the 95th percentile over eight bands."""
    target, scale = max(1, -(-p95 // 8)), 1
    while True:
        for step in (1, 2, 5):
            if step * scale >= target:
                return step * scale
        scale *= 10


class InsightsService(LifecycleService):
    def _projection(self) -> tuple[object, object, dict[str, object]]:
        session, publication, _, provenance = self._context()
        return session, publication, provenance | {'model': 'silver.customer_events projection', 'serving_relation': 'journey events', 'timezone': 'UTC'}

    def activity_heatmap(self) -> dict[str, object]:
        session, publication, provenance = self._projection()
        if not publication:
            return wire({'status': 'unavailable', 'message': UNAVAILABLE, 'provenance': provenance})
        events = f'{publication.database}.events'
        sessions = session._query(f"""SELECT toDayOfWeek(session_start) AS weekday, toHour(session_start) AS hour, count() AS sessions
            FROM (SELECT session_id, min(event_time) AS session_start FROM {events}
                  WHERE source IN ('web','mobile') AND NOT is_bot_session GROUP BY session_id)
            GROUP BY weekday, hour""")
        orders = session._query(f"""SELECT toDayOfWeek(event_time) AS weekday, toHour(event_time) AS order_hour, toHour(event_time) AS hour, count() AS orders
            FROM {events} WHERE event_name='order_created' GROUP BY weekday, order_hour, hour""")
        counts = {(w, h): [0, 0] for w in range(1, 8) for h in range(24)}
        for row in sessions:
            counts[(int(row['weekday']), int(row['hour']))][0] = int(row['sessions'])
        for row in orders:
            counts[(int(row['weekday']), int(row['hour']))][1] = int(row['orders'])
        cells = [{'weekday': w, 'hour': h, 'sessions': s, 'orders': o} for (w, h), (s, o) in counts.items()]
        top = max(cells, key=lambda c: (c['sessions'], -c['weekday'], -c['hour']))
        busiest = {'weekday': top['weekday'], 'hour': top['hour'], 'sessions': top['sessions']} if top['sessions'] else None
        return wire({'status': 'available', 'timezone': 'UTC', 'cells': cells, 'busiest': busiest, 'provenance': provenance})

    def order_values(self) -> dict[str, object]:
        session, publication, provenance = self._projection()
        currency = provenance.get('currency')
        if not publication:
            return wire({'status': 'unavailable', 'currency': currency, 'bands': [], 'message': UNAVAILABLE, 'provenance': provenance})
        events = f'{publication.database}.events'
        summary = session._query(f"""SELECT count() AS orders, sum(toInt128(gross_amount_minor)) AS gross_minor,
            quantilesExact(0.5,0.9,0.95)(gross_amount_minor) AS q FROM {events} WHERE event_name='order_created'""")[0]
        total = int(summary['orders'])
        if not total:
            return wire({'status': 'empty', 'currency': currency, 'orders': 0, 'gross_minor': 0, 'median_minor': None, 'p90_minor': None,
                         'band_width_minor': None, 'bands': [], 'provenance': provenance})
        median, p90, p95 = (int(value) for value in summary['q'])
        width = band_width(p95)
        rows = session._query(f"""SELECT least(intDiv(gross_amount_minor, {{width:Int64}}), 7) AS band, count() AS orders,
            sum(toInt128(gross_amount_minor)) AS gross_minor FROM {events} WHERE event_name='order_created' GROUP BY band ORDER BY band""", {'width': width})
        found = {int(row['band']): row for row in rows}
        bands = [{'from_minor': k * width, 'to_minor': None if k == 7 else (k + 1) * width,
                  'orders': int(found[k]['orders']) if k in found else 0, 'gross_minor': int(found[k]['gross_minor']) if k in found else 0} for k in range(8)]
        return wire({'status': 'available', 'currency': currency, 'orders': total, 'gross_minor': int(summary['gross_minor']),
                     'median_minor': median, 'p90_minor': p90, 'band_width_minor': width, 'bands': bands,
                     'definition': 'Original order gross amounts (refunds excluded); percentiles are exact order values.', 'provenance': provenance})

    def loyalty(self) -> dict[str, object]:
        session, _, as_of, provenance = self._context(detail=False)
        database = session.release.database
        rows = session._query(f"""SELECT if(p.profile_type='customer' AND c.loyalty_tier IN ('gold','silver','bronze'), c.loyalty_tier, 'none') AS tier,
            count() AS customers, sum(toInt128(p.order_count)) AS orders, sum(toInt128(p.net_revenue_minor)) AS net_revenue_minor
            FROM {database}.customer_360 p LEFT JOIN (SELECT customer_id, loyalty_tier FROM {database}.customers) c ON c.customer_id=p.customer_id
            GROUP BY tier""")
        gold_net = int(session._query(f'SELECT sum(toInt128(net_revenue_minor)) AS gold_net FROM {database}.customer_360')[0]['gold_net'])
        found = {row['tier']: row for row in rows}
        tiers = []
        for tier in TIERS:
            row = found.get(tier, {'customers': 0, 'orders': 0, 'net_revenue_minor': 0})
            customers, net = int(row['customers']), int(row['net_revenue_minor'])
            tiers.append({'tier': tier, 'customers': customers, 'orders': int(row['orders']), 'net_revenue_minor': net,
                          'net_per_customer_minor': (abs(net) // customers) * (-1 if net < 0 else 1) if customers else None})
        if sum(t['net_revenue_minor'] for t in tiers) != gold_net:
            raise LifecycleError('reconciliation_failed')
        return wire({'status': 'available', 'currency': provenance.get('currency'), 'tiers': tiers, 'total_net_revenue_minor': gold_net,
                     'definition': 'Latest CRM loyalty tier as of the certified release; anonymous and unmatched profiles have no tier.',
                     'provenance': provenance | {'model': 'gold.customer_360 + customers', 'serving_relation': 'customer_360'}})

    def channels(self) -> dict[str, object]:
        session, publication, provenance = self._projection()
        if not publication:
            return wire({'status': 'unavailable', 'sources': [], 'message': UNAVAILABLE, 'provenance': provenance})
        events = f'{publication.database}.events'
        rows = session._query(f"""WITH origin AS (SELECT session_id, any(source) AS session_source FROM {events}
                WHERE source IN ('web','mobile') AND NOT is_bot_session GROUP BY session_id),
            orders AS (SELECT ifNull(o.session_source, 'unknown') AS source, count() AS orders FROM {events} e
                LEFT JOIN origin o ON o.session_id=e.session_id WHERE e.event_name='order_created' GROUP BY source)
            SELECT s.source AS source, s.sessions AS sessions, ifNull(r.orders, 0) AS orders FROM
                (SELECT session_source AS source, count() AS sessions FROM origin GROUP BY source
                 UNION ALL SELECT 'unknown', 0) s LEFT JOIN orders r ON r.source=s.source
            SETTINGS join_use_nulls=1""")
        merged: dict[str, list[int]] = {}
        for row in rows:
            entry = merged.setdefault(row['source'], [0, 0]); entry[0] += int(row['sessions']); entry[1] = max(entry[1], int(row['orders']))
        sources = [{'source': source, 'sessions': sessions, 'orders': orders, 'conversion_rate': ratio(orders, sessions)}
                   for source, (sessions, orders) in sorted(merged.items(), key=lambda item: ('web', 'mobile', 'unknown').index(item[0]) if item[0] in ('web', 'mobile', 'unknown') else 9)
                   if source != 'unknown' or orders]
        return wire({'status': 'available', 'sources': sources,
                     'definition': 'Orders are matched to the source of their purchase session; unmatched orders stay unknown.', 'provenance': provenance})

    def products(self) -> dict[str, object]:
        """Certified product, brand and device commerce for the pinned release (older releases lack the relation)."""
        session, _, _, provenance = self._context(detail=False)
        database = session.release.database
        provenance = provenance | {'model': 'gold.commerce_daily', 'serving_relation': 'commerce_daily'}
        present = session._query("SELECT count() AS present FROM system.tables WHERE database={database:String} AND name='commerce_daily'",
                                 {'database': database})[0]['present']
        if not int(present):
            return wire({'status': 'unavailable', 'message': 'This certified release predates product commerce; publish a newer release.', 'provenance': provenance})
        check = session._query(f"""SELECT (SELECT sum(toInt128(gross_revenue_minor)) FROM {database}.campaign_performance_daily) AS performance_gross,
            (SELECT sum(toInt128(gross_revenue_minor)) FROM {database}.commerce_daily) AS commerce_gross""")[0]
        if int(check['performance_gross'] or 0) != int(check['commerce_gross'] or 0):
            raise LifecycleError('reconciliation_failed')
        brands = session._query(f"""SELECT category, brand, sum(toInt128(units)) AS units, sum(toInt128(gross_revenue_minor)) AS gross_revenue_minor,
            sum(toInt128(net_revenue_minor)) AS net_revenue_minor FROM {database}.commerce_daily GROUP BY category, brand""")
        categories: dict[str, dict[str, object]] = {}
        for row in brands:
            entry = categories.setdefault(row['category'], {'category': row['category'], 'units': 0, 'gross_revenue_minor': 0, 'net_revenue_minor': 0, 'brands': []})
            for field in ('units', 'gross_revenue_minor', 'net_revenue_minor'):
                entry[field] += int(row[field])
            entry['brands'].append({'brand': row['brand'], 'units': int(row['units']), 'gross_revenue_minor': int(row['gross_revenue_minor']),
                                    'net_revenue_minor': int(row['net_revenue_minor'])})
        ordered = sorted(categories.values(), key=lambda c: (-c['net_revenue_minor'], c['category']))
        for entry in ordered:
            entry['brands'].sort(key=lambda b: (-b['net_revenue_minor'], b['brand']))
        top = session._query(f"""SELECT product_id, any(product_name) AS product_name, any(category) AS category, any(brand) AS brand,
            sum(toInt128(units)) AS units, sum(toInt128(orders)) AS orders, sum(toInt128(net_revenue_minor)) AS net_revenue_minor
            FROM {database}.commerce_daily GROUP BY product_id ORDER BY net_revenue_minor DESC, product_id LIMIT 10""")
        devices = session._query(f"""SELECT device_type, sum(toInt128(units)) AS units, sum(toInt128(gross_revenue_minor)) AS gross_revenue_minor,
            sum(toInt128(net_revenue_minor)) AS net_revenue_minor FROM {database}.commerce_daily GROUP BY device_type""")
        total_gross = sum(int(d['gross_revenue_minor']) for d in devices)
        device_rows = [{'device_type': d['device_type'], 'units': int(d['units']), 'gross_revenue_minor': int(d['gross_revenue_minor']),
                        'net_revenue_minor': int(d['net_revenue_minor']), 'gross_share': ratio(int(d['gross_revenue_minor']), total_gross),
                        'avg_item_value_minor': int(d['gross_revenue_minor']) // int(d['units']) if int(d['units']) else None}
                       for d in sorted(devices, key=lambda d: ('desktop', 'mobile', 'tablet', 'unknown').index(d['device_type']) if d['device_type'] in ('desktop', 'mobile', 'tablet', 'unknown') else 9)]
        return wire({'status': 'available', 'currency': provenance.get('currency'), 'categories': ordered,
                     'top_products': [{k: (int(v) if k in ('units', 'orders', 'net_revenue_minor') else v) for k, v in row.items()} for row in top],
                     'devices': device_rows,
                     'definition': 'Certified order lines by product and purchase device; refunds are allocated to the original items on the refund date.',
                     'provenance': provenance})
