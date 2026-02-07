"""
Minimal smoke script for salmonpy.
"""

import tempfile

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None

from salmonpy import apply_salmon_dictionary, create_salmon_datapackage, infer_dictionary, read_salmon_datapackage, validate_dictionary


def run() -> None:
    if pd is None:
        raise RuntimeError("pandas not installed; install and rerun smoke test.")
    df = pd.DataFrame({"species": ["Coho", "Chinook"], "count": [100, 200]})
    dict_df = infer_dictionary(df, dataset_id="demo", table_id="observations")
    dict_df.loc[dict_df["column_name"] == "count", "column_role"] = "measurement"
    dict_df.loc[dict_df["column_name"] == "count", "value_type"] = "integer"
    dict_df.loc[dict_df["column_name"] == "count", "required"] = True

    validate_dictionary(dict_df)
    applied = apply_salmon_dictionary(df, dict_df)
    assert "count" in applied.columns

    dataset_meta = pd.DataFrame({"dataset_id": ["demo"], "title": ["Demo"], "description": ["Demo dataset"]})
    table_meta = pd.DataFrame(
        {"dataset_id": ["demo"], "table_id": ["observations"], "file_name": ["observations.csv"], "table_label": ["Observations"]}
    )

    tempdir = tempfile.mkdtemp(prefix="salmonpy-smoke-")
    create_salmon_datapackage({"observations": applied}, dataset_meta, table_meta, dict_df, path=tempdir, overwrite=True)
    pkg = read_salmon_datapackage(tempdir)
    assert "observations" in pkg["resources"]


if __name__ == "__main__":
    run()
    print("salmonpy smoke test passed")
