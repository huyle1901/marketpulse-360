"""Observed RFM/behavior evidence; consent gates never infer an identity merge."""
from __future__ import annotations
from collections.abc import Mapping
from datetime import datetime,timezone
from .lifecycle_contracts import Condition


def activation_eligible(profile:Mapping[str,object]) -> bool:
    return profile.get('profile_type')=='customer' and all(profile.get(k) is True for k in ('consent_analytics','crm_matched','crm_consent_analytics','crm_consent_marketing'))


def identity_evidence(profile_type:str) -> dict[str,str]:
    definitions={
        'customer':('deterministic_customer_reference','Existing explicit customer ID or deterministic session stitch; Gold does not distinguish these paths.'),
        'anonymous':('anonymous_device_only','Existing anonymous device reference; no verified person-level join.'),
        'session':('session_only','Existing session reference; no verified person-level join.')}
    if profile_type not in definitions:raise ValueError('unsupported profile type')
    confidence,basis=definitions[profile_type]
    return {'confidence':confidence,'resolution_basis':basis}


def lifecycle_metrics(profile:Mapping[str,object],detail:Mapping[str,object],as_of:datetime) -> dict[str,object]:
    if as_of.tzinfo is None:raise ValueError('as_of requires UTC instant')
    purchase=detail['last_purchase_at']
    if purchase is not None and (not isinstance(purchase,datetime) or purchase.tzinfo is None or purchase>=as_of):raise ValueError('purchase must precede pinned as_of')
    if (profile['order_count']==0)!=(purchase is None):raise ValueError('purchase context differs from Gold frequency')
    recency=(as_of.astimezone(timezone.utc).date()-purchase.astimezone(timezone.utc).date()).days if purchase else None
    return {'last_purchase_at':purchase,'recency_days':recency,'add_to_cart_count':detail['add_to_cart_count'],'email_click_count':detail['email_click_count'],
        'opportunities':{'high_value':profile['order_count']>0 and profile['net_revenue_minor']>=5000000,
                         'churn_risk':profile['order_count']>0 and recency is not None and recency>=30,
                         'cart_opportunity':detail['add_to_cart_count']>0 and profile['order_count']==0}}


def matches_condition(metrics:Mapping[str,object],condition:Condition) -> bool:
    value=metrics.get(condition.field)
    if value is None:return False
    number=int(condition.value)
    return value>=number if condition.op=='gte' else value<=number if condition.op=='lte' else value==number
