# Stakeholder decision tasks and traceability

Use the retained seed-360 app. API readiness is automated; the human sheet below
must be completed by real participants. Record the selected release identity,
date, pseudonymous participant ID, role, task, start/end time, decision, success,
errors/help required and evidence filename. Do not record contact details.

| ID | Stakeholder | Requirement / route | Expected evidence / decision | Automated proof |
|---|---|---|---|---|
| live_kpi | Performance | Live KPI `/` | Compare 14 live keys with certified VND net22,193,000; choose campaign to inspect | app.json; MP06 Kafka projection oracle |
| alerts | Performance | Explained alert `/` | Explain budget-overrun occurrence from recorded threshold/evidence | app.json; MP06 alert tests |
| pacing | CMO | Budget pacing `/` | Spend91,461 for configured campaign/period; distinguish complete historical period | app.json; MP06 pacing math |
| simulation | CMO | Budget simulation `/` | VND1,000 allocation yields35,282 historical projected net; no causal forecast claim | app.json; independent literal oracle |
| funnel | Product | Stage reach `/analysis` | Identify drop and explain campaign-day incidence scope | app.json; MP07 source/Gold oracle |
| attribution | Analyst | Model comparison `/analysis` | First/last/linear preserve22,193,000 net; investigate journey | app.json; MP07 financial conservation |
| customer_360 | CRM | Timeline `/customers` | Inspect pseudonymous activity and confidence; no raw identity leakage | app.json; MP08 timeline oracle |
| segmentation | CRM | Audience rules `/customers` | Preview repeatably; inspect saved revisions; only consent-eligible activation | app.json; MP08 version/consent tests |
| cohorts | CRM | Cohorts `/customers` | Explain governed60-profile population and activity denominators | app.json; MP08 cohort oracle |
| opportunity | CRM | Value/risk `/customers` | Inspect high-value flag; distinguish opportunity from permission to activate | app.json; MP08 independent RFM/opportunity checks |

Human acceptance remains pending. Suggested protocol: one participant per
representative CMO/Performance/CRM/Analyst/Product role, with multiple tasks per
participant where appropriate. Show instructions only, start timing before the
first navigation, allow assistance but record it, and ask the participant to
explain the decision and metric limitations. Repeat unsuccessful tasks after
recording the failure. Report participant selection and study limitations.

| Participant | Role | Task | Release | Seconds | Decision/result | Help/errors | Evidence |
|---|---|---|---|---:|---|---|---|
| pending | pending | pending | pending | | pending | pending | pending |

Group member count: unconfirmed. Requirement selection: ten candidate
capabilities. `2*n` is covered only for n<=5; a larger group needs more real
requirements and implementation before claiming coverage.
