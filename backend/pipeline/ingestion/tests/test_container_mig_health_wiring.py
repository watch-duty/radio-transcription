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

    raise ValueError(f"Unclosed Terraform block: {marker}")


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
            "VM_HEALTH_WORKER_ENDPOINTS="
            "http://127.0.0.1:8081/healthz,"
            "http://127.0.0.1:8082/healthz",
            "VM_HEALTH_PROBE_TIMEOUT_SEC=2.0",
            "VM_HEALTH_PROBE_INTERVAL_SEC=5.0",
            "VM_HEALTH_LISTEN_HOST=0.0.0.0",
            "VM_HEALTH_LISTEN_PORT=8080",
            "VM_HEALTH_HYSTERESIS_SEC=600.0",
        )
        for env_var in expected_env:
            with self.subTest(env_var=env_var):
                self.assertIn(env_var, health_unit)

    def test_nginx_health_aggregator_is_removed(self) -> None:
        cloud_config = _without_comment_lines(_text(_CLOUD_CONFIG))

        self.assertNotIn("/etc/nginx-aggregator/healthz.conf", cloud_config)
        self.assertNotIn("mirror.gcr.io/library/nginx", cloud_config)
        self.assertNotIn("nginx", cloud_config.lower())

    def test_worker_topology_and_host_probe_port_are_preserved(self) -> None:
        cloud_config = _text(_CLOUD_CONFIG)

        self.assertIn(
            "-p 127.0.0.1:$((8080 + %i)):8080",
            cloud_config,
        )
        self.assertIn(
            "systemctl enable --now ${service_name}@1.service",
            cloud_config,
        )
        self.assertIn(
            "systemctl enable --now ${service_name}@2.service",
            cloud_config,
        )
        self.assertIn(
            "systemctl enable --now ${service_name}-health.service",
            cloud_config,
        )
        self.assertIn(
            "iptables -I INPUT -p tcp --dport 8080 -j ACCEPT",
            cloud_config,
        )

    def test_gcp_health_thresholds_unchanged(self) -> None:
        main_tf = _without_comment_lines(_text(_MAIN_TF))
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


if __name__ == "__main__":
    unittest.main()
