import gzip
import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from flowbridge.io import archive, read_document, write_artifacts, MAX_BYTES


class InputTests(unittest.TestCase):
    def test_duplicate_properties_are_rejected(self):
        with self.assertRaises(ValueError):
            read_document(b'{"topic":"one","topic":"two"}')

    def test_gzip_is_bounded_after_expansion(self):
        with self.assertRaises(ValueError):
            read_document(gzip.compress(b" " * (MAX_BYTES + 1)))
        self.assertEqual(read_document(gzip.compress(b'{"ok":true}')), {"ok": True})

    def test_nonfinite_numbers_rejected(self):
        with self.assertRaises(ValueError):
            read_document(b'{"number":NaN}')

    def test_archive_paths_and_determinism(self):
        for path in ("../secret", "/secret", "a/../../secret", "a\\secret", "a/./secret"):
            with self.assertRaises(ValueError):
                archive({path: "x"})
        raw = archive({"a/file.txt": "test"})
        self.assertEqual(raw, archive({"a/file.txt": "test"}))
        self.assertEqual(zipfile.ZipFile(io.BytesIO(raw)).read("a/file.txt"), b"test")

    def test_write_does_not_overwrite_existing_work(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "out"
            write_artifacts({"project/file.txt": "keep"}, directory)
            with self.assertRaises(ValueError):
                write_artifacts({"project/file.txt": "replace"}, directory)
            self.assertEqual((directory / "project/file.txt").read_text(), "keep")
