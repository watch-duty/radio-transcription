from __future__ import annotations

import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

_SCRIPTS_SFT_DIR = str(
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "sft"
)
if _SCRIPTS_SFT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_SFT_DIR)

import build_validation_manifest_from_eval as script_module  # noqa: E402


class TestBuildValidationManifestFromEval(unittest.TestCase):
    def test_build_validation_rows_empty_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "eval_rows must be non-empty"):
            script_module.build_validation_rows([], max_rows=5000, seed=0)

    def test_build_validation_rows_invalid_max_rows_raises(self) -> None:
        eval_rows = [{"audio_gcs_uri": "gs://b/a.wav", "split": "eval"}]
        with self.assertRaisesRegex(ValueError, "max_rows must be positive"):
            script_module.build_validation_rows(eval_rows, max_rows=0, seed=0)

        with self.assertRaisesRegex(ValueError, "max_rows must be positive"):
            script_module.build_validation_rows(eval_rows, max_rows=-10, seed=0)

    def test_build_validation_rows_under_cap_relabels_all(self) -> None:
        eval_rows = [
            {"id": "1", "audio_gcs_uri": "gs://b/1.wav", "split": "eval"},
            {"id": "2", "audio_gcs_uri": "gs://b/2.wav", "split": "eval"},
        ]
        val_rows = script_module.build_validation_rows(
            eval_rows, max_rows=5000, seed=0
        )
        self.assertEqual(len(val_rows), 2)
        self.assertEqual(val_rows[0]["split"], "validation")
        self.assertEqual(val_rows[1]["split"], "validation")
        self.assertEqual(val_rows[0]["id"], "1")
        self.assertEqual(val_rows[1]["id"], "2")

    def test_build_validation_rows_over_cap_samples_reproducibly(self) -> None:
        eval_rows = [
            {"id": str(i), "audio_gcs_uri": f"gs://b/{i}.wav", "split": "eval"}
            for i in range(100)
        ]
        val_rows_1 = script_module.build_validation_rows(
            eval_rows, max_rows=10, seed=42
        )
        val_rows_2 = script_module.build_validation_rows(
            eval_rows, max_rows=10, seed=42
        )

        self.assertEqual(len(val_rows_1), 10)
        self.assertEqual(val_rows_1, val_rows_2)

        # Check all splits are updated to validation
        self.assertTrue(all(r["split"] == "validation" for r in val_rows_1))

        # Check ascending index ordering is maintained
        indices = [int(r["id"]) for r in val_rows_1]
        self.assertEqual(indices, sorted(indices))

    def test_read_and_write_jsonl_local(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = pathlib.Path(tmp_dir) / "test.jsonl"
            rows = [{"a": 1, "split": "eval"}, {"b": 2, "split": "eval"}]
            script_module.write_jsonl(rows, str(tmp_path))

            read_back = script_module.read_jsonl(str(tmp_path))
            self.assertEqual(read_back, rows)

    @patch("google.cloud.storage.Client")
    def test_main_local_paths_no_storage_client(
        self, mock_storage_client: MagicMock
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            eval_file = pathlib.Path(tmp_dir) / "eval.jsonl"
            out_file = pathlib.Path(tmp_dir) / "val.jsonl"

            eval_file.write_text(
                json.dumps({"id": "1", "split": "eval"}) + "\n",
                encoding="utf-8",
            )

            test_args = [
                "script_name",
                "--eval",
                str(eval_file),
                "--out-validation-uri",
                str(out_file),
                "--max-rows",
                "5000",
            ]

            with patch.object(sys, "argv", test_args):
                script_module.main()

            mock_storage_client.assert_not_called()

            val_data = [
                json.loads(line)
                for line in out_file.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(len(val_data), 1)
            self.assertEqual(val_data[0]["split"], "validation")
            self.assertEqual(val_data[0]["id"], "1")
