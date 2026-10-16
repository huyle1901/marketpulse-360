"""Versioned, canonical, allowlisted lifecycle rule contracts."""
from __future__ import annotations
from dataclasses import dataclass,asdict
from datetime import date
import hashlib
import json
import re

SCHEMA_VERSION='mp08-segment-v1'
FIELDS=frozenset(('recency_days','order_count','net_revenue_minor','session_count','event_count','add_to_cart_count','email_click_count'))
OPS=frozenset(('gte','lte','eq'))
PROFILE_TYPES=frozenset(('customer','anonymous','session'))
PURPOSES=frozenset(('analytics','activation'))
OPPORTUNITIES=frozenset(('high_value','churn_risk','cart_opportunity'))


def month(value:str|None) -> date|None:
    if value is None:return None
    if not isinstance(value,str):raise ValueError('month must be ISO first of month')
    try:result=date.fromisoformat(value)
    except ValueError as error:raise ValueError('invalid month') from error
    if result.isoformat()!=value or result.day!=1 or not date(1900,1,1)<=result<=date(2299,12,1):raise ValueError('invalid month boundary')
    return result


def validate_selection(cohort_month:str|None,activity_month:str|None,profile_type:str|None,purpose:str,opportunity:str|None=None,limit:int=20,offset:int=0) -> None:
    month(cohort_month);month(activity_month)
    if profile_type is not None and profile_type not in PROFILE_TYPES:raise ValueError('unsupported profile type')
    if purpose not in PURPOSES or opportunity is not None and opportunity not in OPPORTUNITIES:raise ValueError('unsupported lifecycle selection')
    if type(limit) is not int or not 1<=limit<=100 or type(offset) is not int or not 0<=offset<=100000:raise ValueError('invalid pagination')


@dataclass(frozen=True,order=True)
class Condition:
    field:str
    op:str
    value:str

    def __post_init__(self) -> None:
        if self.field not in FIELDS or self.op not in OPS:raise ValueError('unknown segment field/operator')
        if not isinstance(self.value,str) or len(self.value)>20 or re.fullmatch(r'0|-?[1-9][0-9]*',self.value) is None:raise ValueError('requires canonical integral string')
        number=int(self.value)
        if abs(number)>2**63-1 or number<0 and self.field!='net_revenue_minor':raise ValueError('segment value out of range')


@dataclass(frozen=True)
class SegmentRule:
    purpose:str
    conditions:tuple[Condition,...]=()
    cohort_month:str|None=None
    activity_month:str|None=None
    profile_type:str|None=None

    def __post_init__(self) -> None:
        validate_selection(self.cohort_month,self.activity_month,self.profile_type,self.purpose)
        if not isinstance(self.conditions,tuple) or any(not isinstance(c,Condition) for c in self.conditions) or len(self.conditions)>8:raise ValueError('requires up to eight predicates')
        if len({(c.field,c.op) for c in self.conditions})!=len(self.conditions):raise ValueError('duplicate predicate')
        object.__setattr__(self,'conditions',tuple(sorted(self.conditions)))

    @classmethod
    def from_payload(cls,value:object) -> SegmentRule:
        if not isinstance(value,dict) or set(value)!={'schema_version','purpose','conditions','cohort_month','activity_month','profile_type'} or value['schema_version']!=SCHEMA_VERSION:raise ValueError('invalid segment rule schema')
        if not isinstance(value['conditions'],list):raise ValueError('conditions must be an array')
        try:
            conditions=[]
            for row in value['conditions']:
                if not isinstance(row,dict) or set(row)!={'field','op','value'}:raise ValueError('invalid predicate shape')
                conditions.append(Condition(**row))
            return cls(value['purpose'],tuple(conditions),value['cohort_month'],value['activity_month'],value['profile_type'])
        except (TypeError,KeyError) as error:raise ValueError('invalid segment value') from error

    def payload(self) -> dict[str,object]:
        return {'schema_version':SCHEMA_VERSION,'purpose':self.purpose,'cohort_month':self.cohort_month,
                'activity_month':self.activity_month,'profile_type':self.profile_type,'conditions':[asdict(c) for c in self.conditions]}

    @property
    def digest(self) -> str:
        return hashlib.sha256(json.dumps(self.payload(),sort_keys=True,separators=(',',':')).encode()).hexdigest()
