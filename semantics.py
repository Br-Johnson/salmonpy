from __future__ import annotations

from typing import Callable, Sequence

try:
    import pandas as pd
except ImportError as exc:  # pragma: no cover - import guard
    raise ImportError("salmonpy requires pandas; install via `pip install pandas`.") from exc

import re

from .dictionary import validate_dictionary
from .term_search import find_terms
from .dwc_dp import suggest_dwc_mappings

ROLE_MAP = {
    "term_iri": "variable",
    "property_iri": "property",
    "entity_iri": "entity",
    "unit_iri": "unit",
    "constraint_iri": "constraint",
    "method_iri": "method",
}


def _is_missing(value) -> bool:
    return value is None or (isinstance(value, float) and pd.isna(value)) or (isinstance(value, str) and value == "") or pd.isna(value)


def suggest_semantics(
    df: pd.DataFrame,
    dict_df: pd.DataFrame,
    sources: Sequence[str] = ("ols", "nvs"),
    include_dwc: bool = False,
    max_per_role: int = 3,
    search_fn: Callable = find_terms,
) -> pd.DataFrame:
    """
    Suggest semantic annotations for measurement columns.
    """
    dictionary = validate_dictionary(dict_df, require_iris=False)
    if dictionary.empty:
        dictionary.attrs["semantic_suggestions"] = pd.DataFrame()
        if include_dwc:
            dictionary.attrs["dwc_mappings"] = pd.DataFrame()
        return dictionary

    def _first_query(*values: str) -> str:
        for val in values:
            if not _is_missing(val):
                return str(val)
        return ""

    def _clean_query(value: str) -> str:
        value = re.sub(r"[._]+", " ", value)
        value = re.sub(r"\s+", " ", value)
        return value.strip()

    suggestions = []

    for _, row in dictionary.iterrows():
        if row.get("column_role") != "measurement":
            continue

        raw_query = _first_query(row.get("column_description"), row.get("column_label"), row.get("column_name"))
        query = _clean_query(raw_query)
        for col_name, role_name in ROLE_MAP.items():
            if col_name not in dictionary.columns:
                continue
            if not _is_missing(row[col_name]):
                continue
            role_query = query
            if role_name == "unit":
                role_query = _clean_query(_first_query(row.get("unit_label"), query))
            if not role_query:
                continue
            res = search_fn(role_query, role=role_name, sources=sources)
            if res.empty:
                continue
            res = res.head(max_per_role).copy()
            res["column_name"] = row.get("column_name")
            res["dictionary_role"] = role_name
            suggestions.append(res)

    if suggestions:
        suggestions_df = pd.concat(suggestions, ignore_index=True)
    else:
        suggestions_df = pd.DataFrame(columns=["label", "iri", "source", "ontology", "role", "match_type", "definition", "column_name", "dictionary_role"])

    dictionary.attrs["semantic_suggestions"] = suggestions_df
    if include_dwc:
        dictionary.attrs["dwc_mappings"] = suggest_dwc_mappings(dictionary).attrs.get("dwc_mappings", pd.DataFrame())
    return dictionary


__all__ = ["suggest_semantics"]
