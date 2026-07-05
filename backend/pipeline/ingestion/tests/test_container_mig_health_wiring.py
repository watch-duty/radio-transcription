from __future__ import annotations

import pathlib
import unittest

_CLOUD_CONFIG = pathlib.Path(
    "terraform/modules/container_mig/cloud_config.yaml.tftpl"
)
_MAIN_TF = pathlib.Path("terraform/modules/container_mig/main.tf")
_VARIABLES_TF = pathlib.Path("terraform/modules/container_mig/variables.tf")


def _text(path: pathlib.Path) -> str:
    return path.read_text()


def _without_comment_lines(text: str) -> str:
    lines = []
    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            continue
        lines.append(line)
    return "\n".join(lines)


def _without_terraform_comments(text: str) -> str:  # noqa: PLR0912
    chars = []
    index = 0
    in_string = False
    in_block_comment = False
    escaped = False

    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""

        if in_block_comment:
            if char == "*" and next_char == "/":
                in_block_comment = False
                index += 2
            else:
                if char == "\n":
                    chars.append(char)
                index += 1
            continue

        if in_string:
            chars.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            index += 1
            continue

        if char == '"':
            in_string = True
            chars.append(char)
            index += 1
            continue

        if char == "#":
            while index < len(text) and text[index] != "\n":
                index += 1
            continue

        if char == "/" and next_char == "/":
            while index < len(text) and text[index] != "\n":
                index += 1
            continue

        if char == "/" and next_char == "*":
            in_block_comment = True
            index += 2
            continue

        chars.append(char)
        index += 1

    return "".join(chars)


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
        self.assertNotIn(
            "--env-file /etc/container-env/${service_name}.env",
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
            "VM_HEALTH_LISTEN_PORT=8080",
            "VM_HEALTH_HYSTERESIS_SEC=600.0",
        )
        for env_var in expected_env:
            with self.subTest(env_var=env_var):
                self.assertIn(env_var, health_unit)

        self.assertNotIn(
            "http://127.0.0.1:8081/healthz,http://127.0.0.1:8082/healthz",
            health_unit,
        )

    def test_nginx_health_aggregator_is_removed(self) -> None:
        cloud_config = _without_comment_lines(_text(_CLOUD_CONFIG))

        self.assertNotIn("/etc/nginx-aggregator/healthz.conf", cloud_config)
        self.assertNotIn("mirror.gcr.io/library/nginx", cloud_config)
        self.assertNotIn("nginx", cloud_config.lower())

    def test_worker_topology_is_generated_from_worker_count(self) -> None:
        cloud_config = _text(_CLOUD_CONFIG)

        self.assertIn(
            "-p 127.0.0.1:$((8080 + %i)):8080",
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
        self.assertNotIn(
            "systemctl enable --now ${service_name}@1.service", cloud_config
        )
        self.assertNotIn(
            "systemctl enable --now ${service_name}@2.service", cloud_config
        )
        self.assertIn(
            "systemctl enable --now ${service_name}-health.service",
            cloud_config,
        )
        self.assertIn(
            "iptables -I INPUT -p tcp --dport 8080 -j ACCEPT",
            cloud_config,
        )

    def test_template_inputs_derive_worker_health_contract_from_worker_count(
        self,
    ) -> None:
        main_tf = _text(_MAIN_TF)
        variables_tf = _text(_VARIABLES_TF)

        self.assertIn('variable "worker_count"', variables_tf)
        self.assertIn("worker_indices", main_tf)
        self.assertIn("range(1, var.worker_count + 1)", main_tf)
        self.assertIn("vm_health_worker_endpoints", main_tf)
        self.assertIn("worker_systemd_after_units", main_tf)
        self.assertIn("worker_count", main_tf)

    def test_gcp_health_thresholds_unchanged(self) -> None:
        main_tf = _without_terraform_comments(_text(_MAIN_TF))
        health_check = _terraform_block(
            main_tf,
            'resource "google_compute_health_check" "this"',
        )
        autohealing = _terraform_block(
            main_tf,
            'dynamic "auto_healing_policies"',
        )
        threshold_patterns = (
            (health_check, r"check_interval_sec\s+=\s+30\b"),
            (health_check, r"timeout_sec\s+=\s+10\b"),
            (health_check, r"healthy_threshold\s+=\s+1\b"),
            (health_check, r"unhealthy_threshold\s+=\s+3\b"),
            (health_check, r"port\s+=\s+8080\b"),
            (health_check, r'request_path\s+=\s+"/healthz"'),
            (autohealing, r"initial_delay_sec\s+=\s+300\b"),
        )

        for block, pattern in threshold_patterns:
            with self.subTest(pattern=pattern):
                self.assertRegex(block, pattern)

    def test_enable_autohealing_documents_vm_health_contract(self) -> None:
        variables_tf = _text(_VARIABLES_TF)

        self.assertIn("VM Health", variables_tf)
        self.assertIn("same-image", variables_tf)
        self.assertIn("port 8080", variables_tf)
        self.assertIn("GCP probe sources", variables_tf)
        self.assertIn("all configured worker", variables_tf)


if __name__ == "__main__":
    unittest.main()
