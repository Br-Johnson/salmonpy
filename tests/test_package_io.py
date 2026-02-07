import tempfile
import unittest

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None

if pd is None:
    raise unittest.SkipTest("pandas not installed")

from salmonpy import create_salmon_datapackage, read_salmon_datapackage, validate_dictionary


class PackageIOTests(unittest.TestCase):
    def test_create_and_read_package_roundtrip(self):
        df = pd.DataFrame({"species": ["Coho"], "count": [5]})
        dataset_meta = pd.DataFrame({"dataset_id": ["demo"], "title": ["Demo"], "description": ["desc"]})
        table_meta = pd.DataFrame(
            {"dataset_id": ["demo"], "table_id": ["observations"], "file_name": ["observations.csv"], "table_label": ["Observations"]}
        )
        dict_df = pd.DataFrame(
            {
                "dataset_id": ["demo", "demo"],
                "table_id": ["observations", "observations"],
                "column_name": ["species", "count"],
                "column_label": ["species", "count"],
                "column_description": ["", ""],
                "column_role": ["attribute", "measurement"],
                "value_type": ["string", "integer"],
                "required": [False, False],
            }
        )
        dict_df = validate_dictionary(dict_df)

        tmpdir = tempfile.mkdtemp(prefix="salmonpy-io-")
        create_salmon_datapackage({"observations": df}, dataset_meta, table_meta, dict_df, path=tmpdir, overwrite=True)
        pkg = read_salmon_datapackage(tmpdir)

        self.assertIn("observations", pkg["resources"])
        self.assertFalse(pkg["dictionary"].empty)
        self.assertEqual(pkg["dataset"]["dataset_id"].iloc[0], "demo")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
