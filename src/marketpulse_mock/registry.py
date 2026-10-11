"""Idempotent Apicurio Registry v3 JSON Schema registration."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from .kafka_contracts import TOPIC_CONTRACTS, load_contract_schema


@dataclass(frozen=True, slots=True)
class RegistrationSummary:
    created: int
    unchanged: int
    versioned: int
    failed: int
    errors: tuple[str, ...] = ()

    @property
    def success(self) -> bool:
        return self.failed == 0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def register_contracts(
    registry_url: str,
    contract_dir: Path,
    *,
    group_id: str = "marketpulse",
    timeout: float = 10.0,
    opener: Callable[..., object] = urlopen,
) -> RegistrationSummary:
    """Create missing artifacts and append a version only when content changes."""

    base = registry_url.rstrip("/") + "/apis/registry/v3"
    group = quote(group_id, safe="")
    created = unchanged = versioned = failed = 0
    errors: list[str] = []
    for contract in TOPIC_CONTRACTS.values():
        schema = load_contract_schema(contract_dir, contract)
        artifact = quote(contract.artifact_id, safe="")
        content_url = (
            f"{base}/groups/{group}/artifacts/{artifact}/versions/branch%3Dlatest/content"
        )
        try:
            existing = _get_json(content_url, timeout, opener)
        except HTTPError as error:
            code, reason = error.code, error.reason
            error.close()
            if code != 404:
                failed += 1
                errors.append(f"{contract.artifact_id}: HTTP {code} {reason}")
                continue
            try:
                _post_json(
                    f"{base}/groups/{group}/artifacts",
                    {
                        "artifactId": contract.artifact_id,
                        "artifactType": "JSON",
                        "firstVersion": {
                            "version": "1",
                            "content": {
                                "content": _canonical_text(schema),
                                "contentType": "application/json",
                            },
                        },
                    },
                    timeout,
                    opener,
                )
                created += 1
            except (HTTPError, URLError, OSError, ValueError) as create_error:
                failed += 1
                errors.append(f"{contract.artifact_id}: {create_error}")
                if isinstance(create_error, HTTPError):
                    create_error.close()
            continue
        except (URLError, OSError, ValueError) as error:
            failed += 1
            errors.append(f"{contract.artifact_id}: {error}")
            continue

        if existing == schema:
            unchanged += 1
            continue
        try:
            _post_json(
                f"{base}/groups/{group}/artifacts/{artifact}/versions",
                {
                    "content": {
                        "content": _canonical_text(schema),
                        "contentType": "application/json",
                    }
                },
                timeout,
                opener,
            )
            versioned += 1
        except (HTTPError, URLError, OSError, ValueError) as error:
            failed += 1
            errors.append(f"{contract.artifact_id}: {error}")
            if isinstance(error, HTTPError):
                error.close()
    return RegistrationSummary(
        created, unchanged, versioned, failed, tuple(errors)
    )


def _canonical_text(value: dict[str, object]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _get_json(
    url: str,
    timeout: float,
    opener: Callable[..., object],
) -> dict[str, object]:
    request = Request(url, headers={"Accept": "application/json"}, method="GET")
    with opener(request, timeout=timeout) as response:
        value = json.loads(response.read())
    if not isinstance(value, dict):
        raise ValueError(f"registry returned non-object content for {url}")
    return value


def _post_json(
    url: str,
    payload: dict[str, object],
    timeout: float,
    opener: Callable[..., object],
) -> None:
    request = Request(
        url,
        data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with opener(request, timeout=timeout):
        pass
