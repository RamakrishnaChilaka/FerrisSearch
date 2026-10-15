import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import bulk_memory_probe as probe


class BulkMemoryProbeTests(unittest.TestCase):
    def test_generated_input_is_incremental_and_exact(self):
        stats = {"input_bytes": 0}
        chunks = list(probe.request_body(3, 2 * probe.CHUNK_BYTES + 7, stats))
        self.assertLessEqual(max(map(len, chunks)), probe.CHUNK_BYTES)
        body = b"".join(chunks)
        self.assertEqual(stats["input_bytes"], len(body))
        lines = body.splitlines()
        self.assertEqual(len(lines), 6)
        for position in range(3):
            self.assertEqual(json.loads(lines[position * 2]), {"index": {"_id": "only"}})
            source = json.loads(lines[position * 2 + 1])
            self.assertEqual(source["value"], position)
            self.assertEqual(len(source["padding"]), 2 * probe.CHUNK_BYTES + 7)

    def test_source_hash_includes_untracked_rust_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ["Cargo.toml", "Cargo.lock", "build.rs"]:
                (root / name).write_text(name)
            (root / "src").mkdir()
            (root / "proto").mkdir()
            (root / "src" / "one.rs").write_text("first")
            first = probe.source_hash(root)
            (root / "src" / "new.rs").write_text("untracked")
            self.assertNotEqual(probe.source_hash(root), first)
            second = probe.source_hash(root)
            (root / "src" / "new.rs").write_text("changed")
            self.assertNotEqual(probe.source_hash(root), second)

    def test_process_memory_requires_real_rss_and_high_water_mark(self):
        with patch.object(Path, "read_text", return_value="VmRSS:\t64 kB\nVmHWM:\t128 kB\nRssAnon:\t32 kB\n"):
            self.assertEqual(probe.process_memory(7), {"VmRSS": 65536, "VmHWM": 131072, "RssAnon": 32768})
        with patch.object(Path, "read_text", return_value="VmRSS:\t64 kB\n"):
            with self.assertRaisesRegex(RuntimeError, "Missing RSS"):
                probe.process_memory(7)


if __name__ == "__main__":
    unittest.main()
