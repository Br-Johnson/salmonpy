"""
salmonpy: Python helpers for Salmon Data Packages (SDPs).

This mirrors the metasalmon R package at a feature level so Python users can
infer dictionaries, validate metadata, search ontology terms, and build/read
Frictionless-style Salmon Data Packages.
"""

from .dictionary import (
    apply_salmon_dictionary,
    infer_column_role,
    infer_dictionary,
    infer_value_type,
    validate_dictionary,
)
from .github_io import github_raw_url, read_github_csv, read_github_csv_dir
from .ices_vocab import ices_code_types, ices_codes, ices_find_code_types, ices_find_codes
from .package_io import create_salmon_datapackage, read_salmon_datapackage
from .dwc_dp import suggest_dwc_mappings
from .semantics import suggest_semantics
from .term_search import find_terms, sources_for_role
from .term_deduplication import deduplicate_proposed_terms, suggest_facet_schemes
from .validation import validate_semantics
from .ontology_fetch import fetch_salmon_ontology

__all__ = [
    "apply_salmon_dictionary",
    "create_salmon_datapackage",
    "deduplicate_proposed_terms",
    "fetch_salmon_ontology",
    "find_terms",
    "infer_column_role",
    "infer_dictionary",
    "infer_value_type",
    "github_raw_url",
    "ices_code_types",
    "ices_codes",
    "ices_find_code_types",
    "ices_find_codes",
    "read_salmon_datapackage",
    "read_github_csv",
    "read_github_csv_dir",
    "suggest_dwc_mappings",
    "suggest_facet_schemes",
    "suggest_semantics",
    "sources_for_role",
    "validate_dictionary",
    "validate_semantics",
]
