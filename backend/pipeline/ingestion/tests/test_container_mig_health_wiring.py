from __future__ import annotations

import pathlib
import re
import unittest

_CLOUD_CONFIG = pathlib.Path(
    "terraform/modules/services/pipeline/ingestion/container_mig/cloud_config.yaml.tftpl"
)
_MAIN_TF = pathlib.Path(
    "terraform/modules/services/pipeline/ingestion/container_mig/main.tf"
)
_VARIABLES_TF = pathlib.Path(
    "terraform/modules/services/pipeline/ingestion/container_mig/variables.tf"
)


def _text(path: pathlib.Path) -> str:
    return path.read_text()


def _file_entry(text: str, path: str) -> str:
    start = text.index(f"- path: {path}")
    next_entry = text.find("\n- path:", start + 1)
    if next_entry == -1:
        return text[start:]
    return text[start:next_entry]


def _terraform_block(text: str, marker: str) -> str:
    start = text.index(marker)
    brace_start = text.index("{", start)
    depth = 0
    for index in range(brace_start, len(text)):
        char = text[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]

    msg = f"Unclosed Terraform block: {marker}"
    raise ValueError(msg)


class ContainerMigHealthWiringTests(unittest.TestCase):
    def test_health_unit_runs_same_image_python_vm_health_agent(self) -> None:
        cloud_config = _text(_CLOUD_CONFIG)
        health_unit = _file_entry(
            cloud_config,
            "/etc/systemd/system/${service_name}-health.service",
        )

        self.assertIn("${container_image}", health_unit)
        self.assertIn("--network host", health_unit)
        self.assertIn(
            "python -m backend.pipeline.ingestion.vm_health_agent",
            health_unit,
        )
        self.assertIn(
            "ExecStartPre=/usr/bin/docker-credential-gcr configure-docker",
            health_unit,
        )
        self.assertIn(
            "ExecStartPre=/usr/bin/docker pull ${container_image}",
            health_unit,
        )

    def test_vm_health_unit_has_explicit_non_secret_env_contract(
        self,
    ) -> None:
        cloud_config = _text(_CLOUD_CONFIG)
        health_unit = _file_entry(
            cloud_config,
            "/etc/systemd/system/${service_name}-health.service",
        )

        expected_env = (
            "VM_HEALTH_WORKER_ENDPOINTS=${vm_health_worker_endpoints}",
            "VM_HEALTH_PROBE_TIMEOUT_SEC=2.0",
            "VM_HEALTH_PROBE_INTERVAL_SEC=5.0",
            "VM_HEALTH_LISTEN_HOST=0.0.0.0",
            "VM_HEALTH_LISTEN_PORT=${vm_health_port}",
            "VM_HEALTH_HYSTERESIS_SEC=600.0",
        )
        for env_var in expected_env:
            with self.subTest(env_var=env_var):
                self.assertIn(env_var, health_unit)

    def test_vm_health_unit_documents_timing_contract(self) -> None:
        cloud_config = _text(_CLOUD_CONFIG)
        health_unit = _file_entry(
            cloud_config,
            "/etc/systemd/system/${service_name}-health.service",
        )

        expected_fragments = (
            "VM Health timing contract",
            "2s worker probe timeout",
            "5s probe interval",
            "600s continuous all-workers-down hysteresis",
            "stateless and only needs to close its aiohttp session",
        )
        for fragment in expected_fragments:
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, health_unit)

    def test_vm_health_unit_reasserts_host_firewall_rule(self) -> None:
        cloud_config = _text(_CLOUD_CONFIG)
        health_unit = _file_entry(
            cloud_config,
            "/etc/systemd/system/${service_name}-health.service",
        )
        expected_exec_start_pre = (
            "ExecStartPre=/bin/sh -c 'iptables -C INPUT -p tcp --dport "
            "${vm_health_port} -j ACCEPT || iptables -I INPUT -p tcp --dport "
            "${vm_health_port} -j ACCEPT'"
        )

        self.assertIn(expected_exec_start_pre, health_unit)

    def test_worker_topology_is_generated_from_worker_indices(self) -> None:
        cloud_config = _text(_CLOUD_CONFIG)

        self.assertIn(
            "-p 127.0.0.1:$((${vm_health_port} + %i)):8080",
            cloud_config,
        )
        self.assertIn(
            "%{ for worker_index in worker_indices",
            cloud_config,
        )
        self.assertIn(
            "systemctl enable --now ${service_name}@${worker_index}.service",
            cloud_config,
        )
        self.assertIn(
            "systemctl enable --now ${service_name}-health.service",
            cloud_config,
        )
        self.assertIn(
            "iptables -I INPUT -p tcp --dport ${vm_health_port} -j ACCEPT",
            cloud_config,
        )

    def test_template_inputs_derive_worker_health_contract_from_worker_indices(
        self,
    ) -> None:
        main_tf = _text(_MAIN_TF)

        self.assertIn("vm_health_port = 8080", main_tf)
        self.assertIn("worker_indices", main_tf)
        self.assertIn("worker_indices = [1, 2]", main_tf)
        self.assertIn("vm_health_worker_endpoints", main_tf)
        self.assertIn("worker_systemd_after_units", main_tf)

    def test_gcp_health_thresholds_unchanged(self) -> None:
        main_tf = _text(_MAIN_TF)
        health_check = _terraform_block(
            main_tf,
            'resource "google_compute_health_check" "this"',
        )
        autohealing = _terraform_block(
            main_tf,
            'dynamic "auto_healing_policies"',
        )
        threshold_patterns = (
            (health_check, r"^\s*check_interval_sec\s+=\s+30\b"),
            (health_check, r"^\s*timeout_sec\s+=\s+10\b"),
            (health_check, r"^\s*healthy_threshold\s+=\s+1\b"),
            (health_check, r"^\s*unhealthy_threshold\s+=\s+3\b"),
            (health_check, r"^\s*port\s+=\s+local\.vm_health_port\b"),
            (health_check, r'^\s*request_path\s+=\s+"/healthz"'),
            (autohealing, r"^\s*initial_delay_sec\s+=\s+300\b"),
        )

        for block, pattern in threshold_patterns:
            with self.subTest(pattern=pattern):
                self.assertRegex(block, re.compile(pattern, re.MULTILINE))

    def test_enable_autohealing_documents_vm_health_contract(self) -> None:
        variables_tf = _text(_VARIABLES_TF)

        self.assertIn("VM Health", variables_tf)
        self.assertIn("same-image", variables_tf)
        self.assertIn("port 8080", variables_tf)
        self.assertIn("GCP probe sources", variables_tf)
        self.assertIn("all configured worker", variables_tf)


if __name__ == "__main__":
    unittest.main()
