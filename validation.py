"""
Graceful semantic validation with gap reporting.

This module provides user-friendly validation that reports semantic gaps without
aborting the entire validation run.
"""

from typing import Dict, Optional, List
import pandas as pd
import warnings


def validate_semantics(
    dictionary: pd.DataFrame,
    require_iris: bool = False,
    entity_defaults: Optional[pd.DataFrame] = None,
    vocab_priority: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """
    Validate semantics with graceful gap reporting.

    Ensures structural requirements, adds a `required` column if missing,
    runs validate_dictionary(), and reports missing `term_iri` for measurement
    columns. In default mode validate_dictionary() warns (instead of raising)
    when semantic fields are missing, so the caller can continue.

    Parameters
    ----------
    dictionary : pd.DataFrame
        Dictionary tibble/data frame
    require_iris : bool, default=False
        If True, require IRIs in all semantic fields
    entity_defaults : pd.DataFrame, optional
        Data frame with `table_prefix` and `entity_iri`
        (not applied automatically here but reserved for future use)
    vocab_priority : List[str], optional
        Character vector of vocab sources (reserved for future use)

    Returns
    -------
    Dict[str, pd.DataFrame]
        Dictionary with elements:
        - dict: normalized dictionary with `required` column
        - issues: DataFrame of structural issues (empty if none)
        - missing_terms: DataFrame of measurement rows missing `term_iri`
        - missing_semantics: DataFrame of measurement rows missing any semantic field
          (core + optional)

    Examples
    --------
    >>> import pandas as pd
    >>> from salmonpy import validate_semantics
    >>> dict_df = pd.read_csv("column_dictionary.csv")
    >>> result = validate_semantics(dict_df, require_iris=False)
    >>> print(result['issues'])  # Structural problems
    >>> print(result['missing_terms'])  # Measurements needing term_iri
    >>> # Suggest terms for missing measurements
    >>> if not result['missing_terms'].empty:
    ...     print("Proposed terms:")
    ...     print(result['missing_terms'][['term_label', 'term_definition']])
    """
    # Import dictionary helpers (supports both package and direct module execution)
    try:
        from .dictionary import validate_dictionary, CORE_SEMANTIC_FIELDS, OPTIONAL_SEMANTIC_FIELDS
    except Exception:  # pragma: no cover - compatibility for direct module execution
        from dictionary import validate_dictionary, CORE_SEMANTIC_FIELDS, OPTIONAL_SEMANTIC_FIELDS

    dict_df = dictionary.copy()

    # Add required column if missing
    if 'required' not in dict_df.columns:
        dict_df['required'] = False

    issues = pd.DataFrame()
    missing_terms = pd.DataFrame()
    missing_semantics = pd.DataFrame(columns=["field", "table_id", "column_name"])

    # Try to run validate_dictionary
    try:
        validate_dictionary(dict_df, require_iris=require_iris)
    except Exception as e:
        issues = pd.DataFrame({'message': [str(e)]})

    # Find measurement columns missing semantic fields (core + optional qualifiers)
    semantic_rows = dict_df['column_role'] == 'measurement'
    if semantic_rows.any():
        all_semantic_fields = CORE_SEMANTIC_FIELDS + OPTIONAL_SEMANTIC_FIELDS

        for field in all_semantic_fields:
            if field not in dict_df.columns:
                continue
            missing_mask = semantic_rows & (dict_df[field].isna() | (dict_df[field] == ''))
            if missing_mask.any():
                cols = [c for c in ["table_id", "column_name"] if c in dict_df.columns]
                if not cols:
                    continue
                rows = dict_df.loc[missing_mask, cols].copy()
                rows["field"] = field
                rows = rows[["field", *cols]]
                if "table_id" not in cols:
                    rows["table_id"] = pd.NA
                    rows = rows[["field", "table_id", "column_name"]]
                missing_semantics = pd.concat([missing_semantics, rows[["field", "table_id", "column_name"]]])

    # Find measurement columns missing term_iri
    missing_mask = (
        (dict_df['column_role'] == 'measurement') &
        (dict_df['term_iri'].isna() | (dict_df['term_iri'] == ''))
    )

    if missing_mask.any():
        missing_df = dict_df[missing_mask].copy()

        # Generate suggested term labels and definitions
        missing_df['term_label'] = missing_df['column_name'].str.replace('_', ' ').str.title()
        missing_df['term_definition'] = missing_df['column_description'].fillna('')
        missing_df['term_type'] = 'skos_concept'
        missing_df['suggested_parent_iri'] = 'https://w3id.org/gcdfo/salmon#TargetOrLimitRateOrAbundance'

        # Add notes about source
        missing_df['notes'] = (
            'Derived from ' + missing_df['column_name'] +
            ' in ' + missing_df['table_id'] +
            ' (constraints: ' + missing_df['constraint_iri'].fillna('') + ')'
        )

        missing_terms = missing_df[[
            'term_label', 'term_definition', 'term_type',
            'suggested_parent_iri', 'notes'
        ]]

    return {
        'dict': dict_df,
        'issues': issues,
        'missing_terms': missing_terms,
        'missing_semantics': missing_semantics,
    }


__all__ = ['validate_semantics']
