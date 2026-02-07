from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional

try:
    import pandas as pd
except ImportError as exc:  # pragma: no cover - import guard
    raise ImportError("salmonpy requires pandas; install via `pip install pandas`.") from exc

from .dictionary import validate_dictionary


def _clean(value):
    """
    Normalize pandas/NumPy missing values to None for JSON serialization.
    """
    try:
        import pandas as pd  # type: ignore

        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def create_salmon_datapackage(
    resources: Mapping[str, pd.DataFrame],
    dataset_meta: pd.DataFrame,
    table_meta: pd.DataFrame,
    dict_df: pd.DataFrame,
    codes: Optional[pd.DataFrame] = None,
    path: str = ".",
    format: str = "csv",
    overwrite: bool = False,
) -> Path:
    """
    Write a Salmon Data Package (Frictionless-style) to disk.
    """
    if format != "csv":
        raise ValueError("Only CSV format is supported. Use format='csv'.")

    if not isinstance(dataset_meta, pd.DataFrame) or len(dataset_meta) != 1:
        raise ValueError("dataset_meta must be a single-row DataFrame.")
    if not isinstance(table_meta, pd.DataFrame) or len(table_meta) == 0:
        raise ValueError("table_meta must be a non-empty DataFrame.")
    if not isinstance(resources, Mapping) or len(resources) == 0:
        raise ValueError("resources must be a named mapping of DataFrames.")
    if any(not isinstance(v, pd.DataFrame) for v in resources.values()):
        raise ValueError("All resources must be pandas DataFrames.")

    dict_valid = validate_dictionary(dict_df, require_iris=False)

    target = Path(path)
    if target.exists() and not overwrite:
        raise FileExistsError(f"Directory {target} already exists. Set overwrite=True to replace.")
    target.mkdir(parents=True, exist_ok=True)

    dataset_id = dataset_meta["dataset_id"].iloc[0]

    # Write resources and build resource metadata entries
    resource_entries = []
    for resource_name, resource_df in resources.items():
        table_info = table_meta[table_meta["table_id"] == resource_name]
        if table_info.empty:
            continue
        file_name = table_info["file_name"].iloc[0] if "file_name" in table_info else f"{resource_name}.{format}"
        if not file_name:
            file_name = f"{resource_name}.{format}"
        file_path = target / file_name
        resource_df.to_csv(file_path, index=False)

        table_dict = dict_valid[
            (dict_valid["dataset_id"] == dataset_id) & (dict_valid["table_id"] == resource_name)
        ]
        fields = []
        for _, row in table_dict.iterrows():
            field = {
                "name": _clean(row["column_label"]),
                "type": _clean(row["value_type"]),
                "description": _clean(row["column_description"]),
            }
            for optional_key in [
                "unit_iri",
                "term_iri",
                "term_type",
                "property_iri",
                "entity_iri",
                "constraint_iri",
                "method_iri",
            ]:
                value = row.get(optional_key)
                if pd.notna(value) and value not in ("", None):
                    field[optional_key] = _clean(value)
            fields.append(field)

        resource_entries.append(
            {
                "name": resource_name,
                "path": file_name,
                "profile": "data-resource",
                "schema": {"fields": fields},
            }
        )

    datapackage = {
        "profile": "data-package",
        "name": _clean(dataset_id),
        "title": _clean(dataset_meta.get("title", pd.Series([None])).iloc[0]),
        "description": _clean(dataset_meta.get("description", pd.Series([None])).iloc[0]),
        "resources": resource_entries,
    }

    # Optional metadata
    for key in ["creator", "license"]:
        if key in dataset_meta and pd.notna(dataset_meta[key].iloc[0]):
            datapackage[key] = _clean(dataset_meta[key].iloc[0])
    if "temporal_start" in dataset_meta and pd.notna(dataset_meta["temporal_start"].iloc[0]):
        datapackage["temporal"] = {"start": _clean(dataset_meta["temporal_start"].iloc[0])}
        if "temporal_end" in dataset_meta and pd.notna(dataset_meta["temporal_end"].iloc[0]):
            datapackage["temporal"]["end"] = _clean(dataset_meta["temporal_end"].iloc[0])

    with (target / "datapackage.json").open("w", encoding="utf-8") as fp:
        json.dump(datapackage, fp, indent=2)

    # Optional codes
    if codes is not None:
        codes.to_csv(target / "codes.csv", index=False)

    return target


def read_salmon_datapackage(path: str) -> Dict[str, object]:
    """
    Read a Salmon Data Package from disk.
    """
    target = Path(path)
    if not target.exists():
        raise FileNotFoundError(f"Directory {target} does not exist.")

    json_path = target / "datapackage.json"
    if not json_path.exists():
        raise FileNotFoundError(f"{json_path} does not exist.")

    with json_path.open("r", encoding="utf-8") as fp:
        datapackage = json.load(fp)

    dataset_meta = pd.DataFrame(
        {
            "dataset_id": [datapackage.get("name")],
            "title": [datapackage.get("title")],
            "description": [datapackage.get("description")],
            "creator": [datapackage.get("creator")],
            "license": [datapackage.get("license")],
            "temporal_start": [datapackage.get("temporal", {}).get("start") if datapackage.get("temporal") else None],
            "temporal_end": [datapackage.get("temporal", {}).get("end") if datapackage.get("temporal") else None],
        }
    )

    resources = {}
    table_rows = []
    dict_rows = []

    for resource in datapackage.get("resources", []):
        resource_name = resource.get("name")
        file_path = target / resource.get("path", "")
        if not file_path.exists():
            continue
        resource_df = pd.read_csv(file_path)
        resources[resource_name] = resource_df

        table_rows.append(
            {
                "dataset_id": datapackage.get("name"),
                "table_id": resource_name,
                "file_name": resource.get("path"),
                "table_label": resource_name,
                "description": None,
                "observation_unit": None,
                "observation_unit_iri": None,
                "primary_key": None,
            }
        )

        if resource.get("schema", {}).get("fields"):
            for field in resource["schema"]["fields"]:
                dict_rows.append(
                    {
                        "dataset_id": datapackage.get("name"),
                        "table_id": resource_name,
                        "column_name": field.get("name"),
                        "column_label": field.get("name"),
                        "column_description": field.get("description"),
                        "column_role": None,
                        "value_type": field.get("type", "string"),
                        "unit_label": None,
                        "unit_iri": field.get("unit_iri"),
                        "term_iri": field.get("term_iri"),
                        "term_type": field.get("term_type"),
                        "required": False,
                        "property_iri": field.get("property_iri"),
                        "entity_iri": field.get("entity_iri"),
                        "constraint_iri": field.get("constraint_iri"),
                        "method_iri": field.get("method_iri"),
                    }
                )

    table_meta = pd.DataFrame(table_rows)
    dictionary = pd.DataFrame(dict_rows)

    codes = None
    codes_path = target / "codes.csv"
    if codes_path.exists():
        codes = pd.read_csv(codes_path)

    return {
        "dataset": dataset_meta,
        "tables": table_meta,
        "dictionary": dictionary,
        "codes": codes,
        "resources": resources,
    }


__all__ = ["create_salmon_datapackage", "read_salmon_datapackage"]
