"""Print governed Silver sessions by traffic source and email sends by type x month as JSON.

Run inside spark-lakehouse (Silver is only readable through Spark), then compare with
``story_demo_oracle.py --silver``:

    "${dc[@]}" run --rm --no-deps -v "$PWD/scripts:/opt/marketpulse/oracle:ro" \
      --entrypoint /opt/spark/bin/spark-submit spark-lakehouse \
      /opt/marketpulse/oracle/story_demo_silver_mix.py <namespace> 2026-08-31 | grep '^{' > silver.json
"""

import json
import sys
from datetime import date

from marketpulse_lakehouse.session import create_spark_session
from marketpulse_lakehouse.silver import validate_namespace

namespace = validate_namespace(sys.argv[1])
through = date.fromisoformat(sys.argv[2]).isoformat()
spark = create_spark_session("story-demo-silver-mix")
table = f"lakehouse.`{namespace}`.customer_events"
out: dict[str, dict[str, int]] = {"sessions": {}, "email_sends": {}}
for source, count in spark.sql(
    f"SELECT coalesce(get_json_object(payload_json, '$.traffic_source'), 'paid'), count(DISTINCT session_id) "
    f"FROM {table} WHERE event_name = 'page_view' AND event_date <= DATE '{through}' GROUP BY 1"
).collect():
    out["sessions"][source] = count
for email_type, month, count in spark.sql(
    f"SELECT coalesce(get_json_object(payload_json, '$.email_type'), 'untyped'), date_format(event_date, 'yyyy-MM'), "
    f"count(*) FROM {table} WHERE event_name = 'email_sent' AND event_date <= DATE '{through}' GROUP BY 1, 2"
).collect():
    out["email_sends"][f"{email_type} {month}"] = count
print(json.dumps(out, sort_keys=True))
spark.stop()
