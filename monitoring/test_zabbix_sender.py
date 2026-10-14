import json
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import patch

import zabbix_sender

EXPECTED_VALUES = (
    len(zabbix_sender.COUNTER_FIELDS) + len(zabbix_sender.TEXT_FIELDS) + 2
)


def sample_status():
    status = {
        "schema_version": 5,
        "state": "running",
        "updated_at": "2026-10-05T12:00:00.000Z",
        "started_at": "2026-10-05T11:00:00.000Z",
        "uptime_seconds": 3600,
        "last_input_at": None,
        "last_flush_at": None,
        "last_error": 'Redis "connection" failed\nretrying',
        "input_payload_bytes_total": 8192,
        "output_payload_bytes_total": 4096,
        "conflated_payload_bytes_total": 1024,
        "deduplicated_messages_total": 3,
        "deduplicated_payload_bytes_total": 128,
        "outputs": {
            "output2": {"messages_total": 2, "errors_total": 0},
            "output1": {"messages_total": 12, "errors_total": 1},
        },
    }
    for field in zabbix_sender.COUNTER_FIELDS:
        status.setdefault(field, 0)
    return status


class ZabbixSenderTests(unittest.TestCase):
    def test_requires_global_deduplication_counters(self):
        for field in (
            "deduplicated_messages_total",
            "deduplicated_payload_bytes_total",
        ):
            with self.subTest(field=field):
                status = sample_status()
                del status[field]

                with self.assertRaisesRegex(ValueError, field):
                    zabbix_sender.validate_status(status)

    def test_builds_one_numeric_and_quoted_text_value_per_item(self):
        payload = zabbix_sender.build_sender_input(
            "Redis Service", "redis_conflated_pubsub.", sample_status(), 1
        )
        lines = payload.splitlines()

        self.assertEqual(len(lines), EXPECTED_VALUES)
        self.assertIn('"Redis Service" redis_conflated_pubsub.health 1', lines)
        self.assertIn(
            '"Redis Service" redis_conflated_pubsub.input_messages_total 0', lines
        )
        self.assertIn(
            '"Redis Service" redis_conflated_pubsub.input_payload_bytes_total 8192',
            lines,
        )
        self.assertIn(
            '"Redis Service" redis_conflated_pubsub.output_payload_bytes_total 4096',
            lines,
        )
        self.assertIn(
            '"Redis Service" redis_conflated_pubsub.conflated_payload_bytes_total 1024',
            lines,
        )
        self.assertIn(
            '"Redis Service" redis_conflated_pubsub.deduplicated_messages_total 3',
            lines,
        )
        self.assertIn(
            '"Redis Service" redis_conflated_pubsub.deduplicated_payload_bytes_total 128',
            lines,
        )
        self.assertIn(
            '"Redis Service" redis_conflated_pubsub.state "running"', lines
        )
        self.assertIn(
            '"Redis Service" redis_conflated_pubsub.last_input_at "never"', lines
        )
        self.assertIn(
            '"Redis Service" redis_conflated_pubsub.last_error '
            '"Redis \\"connection\\" failed retrying"',
            lines,
        )
        self.assertIn(
            '"Redis Service" redis_conflated_pubsub.outputs_json '
            '"{\\"output1\\":{\\"errors_total\\":1,\\"messages_total\\":12},'
            '\\"output2\\":{\\"errors_total\\":0,\\"messages_total\\":2}}"',
            lines,
        )

    @patch.object(zabbix_sender.subprocess, "run")
    def test_sends_all_values_in_one_zabbix_sender_call(self, run):
        run.return_value = subprocess.CompletedProcess(
            args=["zabbix_sender"],
            returncode=0,
            stdout="processed: {}; failed: 0; total: {}\n".format(
                EXPECTED_VALUES, EXPECTED_VALUES
            ),
            stderr="",
        )
        config = {
            "sender_path": "zabbix_sender",
            "zabbix_server": "zabbix.example.net",
            "zabbix_port": 10051,
            "timeout_seconds": 5.0,
        }
        payload = zabbix_sender.build_sender_input(
            "monitored-host",
            "redis_conflated_pubsub.",
            sample_status(),
            1,
        )

        zabbix_sender.send_batch(config, payload, EXPECTED_VALUES)

        run.assert_called_once()
        self.assertEqual(run.call_args.kwargs["input"], payload)
        self.assertEqual(len(payload.splitlines()), EXPECTED_VALUES)
        self.assertIn("-i", run.call_args.args[0])
        self.assertEqual(run.call_args.kwargs["timeout"], 5.0)

    @patch.object(zabbix_sender.subprocess, "run")
    def test_agent_config_adds_dash_c_and_optional_inheritance(self, run):
        run.return_value = subprocess.CompletedProcess(
            args=["zabbix_sender"], returncode=0, stdout="processed: 1; failed: 0; total: 1\n", stderr=""
        )
        config = {
            "sender_path": "zabbix_sender",
            "zabbix_config": "/etc/zabbix/zabbix_agentd.conf",
            "zabbix_server": None,
            "zabbix_port": 10051,
            "source_address": "10.0.0.5",
            "timeout_seconds": 5.0,
        }

        zabbix_sender.send_batch(config, "x", 1)

        args = run.call_args.args[0]
        self.assertEqual(args[0], "zabbix_sender")
        self.assertEqual(args[1:3], ["-c", "/etc/zabbix/zabbix_agentd.conf"])
        # No `-z`: the server address is inherited from the agent config.
        self.assertNotIn("-z", args)
        self.assertEqual(args[3:5], ["-p", "10051"])
        self.assertEqual(args[5:7], ["-I", "10.0.0.5"])

    def test_zabbix_server_becomes_optional_with_an_agent_config(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "base_url": "http://127.0.0.1:9090/",
                        "host": "monitored-host",
                        "sender_path": "zabbix_sender",
                        "zabbix_config": "/etc/zabbix/zabbix_agentd.conf",
                    }
                ),
                encoding="utf-8",
            )
            loaded = zabbix_sender.load_config(path)

        self.assertEqual(loaded["zabbix_config"], "/etc/zabbix/zabbix_agentd.conf")
        self.assertIsNone(loaded["zabbix_server"])
        self.assertIsNone(loaded["source_address"])

    def test_requires_a_server_when_no_agent_config_is_given(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "base_url": "http://127.0.0.1:9090/",
                        "host": "monitored-host",
                        "sender_path": "zabbix_sender",
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "zabbix_server"):
                zabbix_sender.load_config(path)

    def test_preserves_named_output_payload_metrics_in_compact_json(self):
        status = sample_status()
        status["outputs"]["output1"].update(
            {
                "published_payload_bytes_total": 4096,
                "conflated_payload_bytes_total": 1024,
                "deduplicated_messages_total": 2,
                "deduplicated_payload_bytes_total": 256,
                "payload_reduction_percent": 75.0,
            }
        )

        payload = zabbix_sender.build_sender_input(
            "monitored-host", "redis_conflated_pubsub.", status, 1
        )
        outputs_line = next(
            line for line in payload.splitlines() if "outputs_json " in line
        )
        encoded_outputs = outputs_line.partition("outputs_json ")[2]
        outputs_json = json.loads(encoded_outputs)

        self.assertEqual(json.loads(outputs_json), status["outputs"])

    @patch.object(zabbix_sender, "fetch_json")
    def test_derives_health_from_the_single_root_status_endpoint(self, fetch_json):
        fetch_json.return_value = (200, sample_status())

        status, health = zabbix_sender.collect("http://127.0.0.1:9090/", 1.0)

        self.assertEqual(status["state"], "running")
        self.assertEqual(health, 1)
        fetch_json.assert_called_once_with("http://127.0.0.1:9090/", 1.0)

    @patch.object(zabbix_sender, "fetch_json")
    def test_derives_unhealthy_state_from_status_json(self, fetch_json):
        status_response = sample_status()
        status_response["state"] = "stopped"
        fetch_json.return_value = (200, status_response)

        status, health = zabbix_sender.collect("http://127.0.0.1:9090", 1.0)

        self.assertEqual(status["state"], "stopped")
        self.assertEqual(health, 0)

    @patch.object(zabbix_sender, "fetch_json")
    def test_does_not_request_status_or_health_routes(self, fetch_json):
        for route in ("status", "health"):
            with self.subTest(route=route):
                with self.assertRaises(ValueError):
                    zabbix_sender.collect("http://127.0.0.1:9090/" + route, 1.0)
        fetch_json.assert_not_called()

    @patch.object(zabbix_sender, "urlopen")
    def test_fetches_root_using_http_get(self, urlopen):
        response = urlopen.return_value.__enter__.return_value
        response.getcode.return_value = 200
        response.read.return_value = b"{}"

        zabbix_sender.fetch_json("http://127.0.0.1:9090/", 1.0)

        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "http://127.0.0.1:9090/")
        self.assertEqual(request.get_method(), "GET")

    @patch.object(zabbix_sender, "fetch_json")
    def test_status_outage_produces_only_an_unhealthy_value(self, fetch_json):
        fetch_json.return_value = (503, None)

        status, health = zabbix_sender.collect("http://127.0.0.1:9090", 1.0)

        self.assertIsNone(status)
        self.assertEqual(health, 0)
        self.assertEqual(
            zabbix_sender.build_health_input(
                "monitored-host", "redis_conflated_pubsub.", health
            ),
            '"monitored-host" redis_conflated_pubsub.health 0\n',
        )

    @patch.object(zabbix_sender, "fetch_json")
    def test_invalid_status_schema_sends_no_counter_values(self, fetch_json):
        fetch_json.return_value = (200, {"state": "running"})

        status, health = zabbix_sender.collect("http://127.0.0.1:9090", 1.0)

        self.assertIsNone(status)
        self.assertEqual(health, 0)
        payload = zabbix_sender.build_health_input(
            "monitored-host", "redis_conflated_pubsub.", health
        )
        self.assertEqual(
            payload.splitlines(),
            ['"monitored-host" redis_conflated_pubsub.health 0'],
        )

    @patch.object(zabbix_sender, "fetch_json")
    def test_rejects_outputs_that_are_not_a_map_of_metric_objects(self, fetch_json):
        for outputs in ([], {"output1": 5}):
            with self.subTest(outputs=outputs):
                status_response = sample_status()
                status_response["outputs"] = outputs
                fetch_json.return_value = (200, status_response)

                status, health = zabbix_sender.collect(
                    "http://127.0.0.1:9090", 1.0
                )

                self.assertIsNone(status)
                self.assertEqual(health, 0)

    @patch.object(zabbix_sender.sys, "argv", ["zabbix_sender.py"])
    @patch.object(zabbix_sender, "load_config")
    @patch.object(zabbix_sender, "collect", return_value=(None, 0))
    @patch.object(zabbix_sender, "send_batch")
    def test_main_sends_only_health_when_root_request_fails(
        self, send_batch, collect, load_config
    ):
        config = {
            "base_url": "http://127.0.0.1:9090/",
            "host": "monitored-host",
            "key_prefix": "redis_conflated_pubsub.",
            "timeout_seconds": 5.0,
        }
        load_config.return_value = config

        self.assertEqual(zabbix_sender.main(), 0)

        collect.assert_called_once_with(config["base_url"], config["timeout_seconds"])
        send_batch.assert_called_once_with(
            config,
            '"monitored-host" redis_conflated_pubsub.health 0\n',
            1,
        )


class TemplateParityTests(unittest.TestCase):
    """The shipped templates must expose exactly the keys the sender emits.

    Status counters are added over time; without this check a new counter can be
    collected by the sender yet have no matching template item, so it would be
    silently dropped by Zabbix.
    """

    def emitted_keys(self):
        prefix = zabbix_sender.DEFAULT_KEY_PREFIX
        return (
            {prefix + "health", prefix + zabbix_sender.OUTPUTS_JSON_FIELD}
            | {prefix + field for field in zabbix_sender.COUNTER_FIELDS}
            | {prefix + field for field in zabbix_sender.TEXT_FIELDS}
        )

    def test_templates_match_the_sender_key_set(self):
        for template in ("zabbix_template_5.0.xml", "zabbix_template_7.0.xml"):
            with self.subTest(template=template):
                path = Path(__file__).with_name(template)
                keys = {
                    element.text
                    for element in ET.parse(path).iter("key")
                    if element.text
                }

                self.assertEqual(keys, self.emitted_keys())


if __name__ == "__main__":
    unittest.main()
