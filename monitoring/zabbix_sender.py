#!/usr/bin/env python3
"""Collect Redis Conflated Pub/Sub status and submit it to Zabbix."""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen


DEFAULT_CONFIG = Path(__file__).resolve().with_name("config.json")
DEFAULT_KEY_PREFIX = "redis_conflated_pubsub."
COUNTER_FIELDS = (
    "input_messages_total",
    "input_payload_bytes_total",
    "output_batches_total",
    "output_messages_total",
    "conflated_messages_total",
    "excluded_messages_total",
    "dropped_messages_total",
    "dropped_payload_bytes_total",
    "truncated_messages_total",
    "truncated_payload_bytes_total",
    "publish_errors_total",
    "publish_error_messages_total",
    "uncertain_transactions_total",
    "uncertain_messages_total",
    "input_reconnects_total",
    "output_reconnects_total",
    "pending_keys",
    "uptime_seconds",
    "schema_version",
)
TEXT_FIELDS = (
    "state",
    "updated_at",
    "started_at",
    "last_input_at",
    "last_flush_at",
    "last_error",
)
OUTPUTS_FIELD = "outputs"
OUTPUTS_JSON_FIELD = "outputs_json"
KEY_PREFIX_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
SENDER_SUMMARY_PATTERN = re.compile(
    r"processed:\s*(\d+);\s*failed:\s*(\d+);\s*total:\s*(\d+)",
    re.IGNORECASE,
)


def load_config(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as config_file:
        config = json.load(config_file)
    if not isinstance(config, dict):
        raise ValueError("configuration must be a JSON object")

    required_strings = ("base_url", "zabbix_server", "host", "sender_path")
    for name in required_strings:
        if not isinstance(config.get(name), str) or not config[name].strip():
            raise ValueError("configuration field {!r} must be a non-empty string".format(name))

    prefix = config.get("key_prefix", DEFAULT_KEY_PREFIX)
    if not isinstance(prefix, str) or not KEY_PREFIX_PATTERN.fullmatch(prefix):
        raise ValueError("key_prefix may contain only letters, digits, dots, underscores, and hyphens")
    config["key_prefix"] = prefix

    port = config.get("zabbix_port", 10051)
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("zabbix_port must be an integer between 1 and 65535")
    config["zabbix_port"] = port

    timeout = config.get("timeout_seconds", 5)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
        raise ValueError("timeout_seconds must be a positive number")
    config["timeout_seconds"] = float(timeout)
    return config


def fetch_json(url: str, timeout: float) -> Tuple[int, Any]:
    request = Request(url, headers={"Accept": "application/json"}, method="GET")
    try:
        with urlopen(request, timeout=timeout) as response:
            status_code = response.getcode()
            body = response.read().decode("utf-8")
    except HTTPError as error:
        return error.code, None
    if not body.strip():
        return status_code, None
    return status_code, json.loads(body)


def derive_health(status: Dict[str, Any]) -> int:
    return int(status.get("state") == "running")


def root_endpoint_url(base_url: str) -> str:
    parsed = urlsplit(base_url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("base_url must be an HTTP or HTTPS URL")
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise ValueError(
            "base_url must point to the root URL without a path, query, or fragment"
        )
    return urlunsplit((parsed.scheme, parsed.netloc, "/", "", ""))


def validate_status(status: Dict[str, Any]) -> None:
    missing_fields = [field for field in COUNTER_FIELDS + TEXT_FIELDS if field not in status]
    if OUTPUTS_FIELD not in status:
        missing_fields.append(OUTPUTS_FIELD)
    if missing_fields:
        raise ValueError(
            "status response is missing fields: {}".format(", ".join(missing_fields))
        )

    for field in COUNTER_FIELDS:
        value = status[field]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("status field {!r} must be a non-negative integer".format(field))
    if not isinstance(status["state"], str):
        raise ValueError("status field 'state' must be a string")
    for field in ("updated_at", "started_at", "last_input_at", "last_flush_at", "last_error"):
        value = status[field]
        if value is not None and not isinstance(value, str):
            raise ValueError("status field {!r} must be a string or null".format(field))

    outputs = status[OUTPUTS_FIELD]
    if not isinstance(outputs, dict):
        raise ValueError("status field 'outputs' must be an object keyed by output name")
    for name, metrics in outputs.items():
        if not isinstance(name, str) or not isinstance(metrics, dict):
            raise ValueError("each status output must be an object keyed by its output name")
    try:
        compact_outputs_json(outputs)
    except (TypeError, ValueError) as error:
        raise ValueError("status field 'outputs' must contain valid JSON values") from error


def compact_outputs_json(outputs: Dict[str, Any]) -> str:
    return json.dumps(
        outputs,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def collect(base_url: str, timeout: float) -> Tuple[Optional[Dict[str, Any]], int]:
    endpoint_url = root_endpoint_url(base_url)
    try:
        status_code, status = fetch_json(endpoint_url, timeout)
    except (OSError, ValueError, TimeoutError):
        return None, 0
    if status_code < 200 or status_code >= 300:
        return None, 0
    if not isinstance(status, dict):
        return None, 0

    try:
        validate_status(status)
    except ValueError:
        return None, 0

    return status, derive_health(status)


def sender_value(value: Any, empty_value: str = "-") -> str:
    if value is None:
        value = empty_value
    text = str(value)
    text = " ".join(text.replace("\r", " ").replace("\n", " ").replace("\t", " ").split())
    text = "".join(character for character in text if character.isprintable())
    return '"{}"'.format(text.replace("\\", "\\\\").replace('"', '\\"'))


def build_sender_input(host: str, prefix: str, status: Dict[str, Any], health: int) -> str:
    values: List[Tuple[str, Any]] = [("health", health)]
    values.extend((field, status[field]) for field in COUNTER_FIELDS)
    values.extend((field, status[field]) for field in TEXT_FIELDS)
    values.append((OUTPUTS_JSON_FIELD, compact_outputs_json(status[OUTPUTS_FIELD])))
    values = [
        (field, value[:2048] if field == "last_error" and isinstance(value, str) else value)
        for field, value in values
    ]
    quoted_host = sender_value(host)
    lines = [
        "{} {}{} {}".format(
            quoted_host,
            prefix,
            field,
            str(value)
            if field == "health" or field in COUNTER_FIELDS
            else sender_value(value, "none" if field == "last_error" else "never"),
        )
        for field, value in values
    ]
    return "\n".join(lines) + "\n"


def build_health_input(host: str, prefix: str, health: int) -> str:
    return "{} {}health {}\n".format(sender_value(host), prefix, health)


def send_batch(config: Dict[str, Any], payload: str, expected_count: int) -> None:
    command = [
        config["sender_path"],
        "-z",
        config["zabbix_server"],
        "-p",
        str(config["zabbix_port"]),
        "-i",
        "-",
    ]
    result = subprocess.run(
        command,
        input=payload,
        text=True,
        encoding="utf-8",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=config["timeout_seconds"],
    )
    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.stderr:
        sys.stderr.write(result.stderr)
    if result.returncode != 0:
        raise RuntimeError("zabbix_sender exited with status {}".format(result.returncode))

    summary = SENDER_SUMMARY_PATTERN.search(result.stdout + "\n" + result.stderr)
    if not summary:
        raise RuntimeError("zabbix_sender did not return a processing summary")
    processed, failed, total = (int(value) for value in summary.groups())
    if failed or processed != total or total != expected_count:
        raise RuntimeError(
            "zabbix_sender accepted {}/{} values and rejected {}".format(
                processed, total, failed
            )
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="JSON configuration file (default: %(default)s)",
    )
    args = parser.parse_args()

    try:
        config = load_config(args.config)
        status, health = collect(config["base_url"], config["timeout_seconds"])
        if status is None:
            payload = build_health_input(config["host"], config["key_prefix"], health)
            send_batch(config, payload, 1)
            print(
                "status endpoint unavailable; sent only the health value",
                file=sys.stderr,
            )
        else:
            payload = build_sender_input(
                config["host"], config["key_prefix"], status, health
            )
            send_batch(config, payload, len(COUNTER_FIELDS) + len(TEXT_FIELDS) + 2)
    except (OSError, ValueError, RuntimeError, URLError, subprocess.SubprocessError) as error:
        print("monitoring collection failed: {}".format(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
