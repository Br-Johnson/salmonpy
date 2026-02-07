import unittest

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None

if pd is None:
    raise unittest.SkipTest("pandas not installed")

from salmonpy import infer_dictionary, suggest_semantics
from salmonpy.dwc_dp import suggest_dwc_mappings


class SemanticsTests(unittest.TestCase):
    def test_suggest_semantics_uses_search_fn(self):
        df = pd.DataFrame({"count": [1, 2]})
        dict_df = infer_dictionary(df, dataset_id="demo", table_id="observations")
        dict_df.loc[dict_df["column_name"] == "count", "column_role"] = "measurement"

        def stub_search(query, role=None, sources=None):
            return pd.DataFrame({"label": ["Count"], "iri": ["iri:1"], "source": ["stub"], "ontology": ["o"], "role": [role], "match_type": [""], "definition": [""]})

        enriched = suggest_semantics(df, dict_df, search_fn=stub_search)
        suggestions = enriched.attrs.get("semantic_suggestions")
        self.assertIsNotNone(suggestions)
        self.assertEqual(suggestions.iloc[0]["dictionary_role"], "variable")

    def test_suggest_semantics_uses_role_specific_query(self):
        df = pd.DataFrame({"count": [1, 2]})
        dict_df = infer_dictionary(df, dataset_id="demo", table_id="observations")
        dict_df.loc[dict_df["column_name"] == "count", ["column_role", "unit_label"]] = ["measurement", "fish"]

        queries = []

        def stub_search(query, role=None, sources=None):
            queries.append((query, role))
            return pd.DataFrame({"label": ["Count"], "iri": ["iri:1"], "source": ["stub"], "ontology": ["o"], "role": [role], "match_type": [""], "definition": [""]})

        suggest_semantics(df, dict_df, search_fn=stub_search)
        self.assertIn(("fish", "unit"), queries)

    def test_suggest_semantics_can_include_dwc(self):
        df = pd.DataFrame({"count": [1, 2]})
        dict_df = infer_dictionary(df, dataset_id="demo", table_id="observations")
        dict_df.loc[dict_df["column_name"] == "count", "column_role"] = "measurement"

        def stub_search(query, role=None, sources=None):
            return pd.DataFrame({"label": ["Count"], "iri": ["iri:1"], "source": ["stub"], "ontology": ["o"], "role": [role], "match_type": [""], "definition": [""]})

        enriched = suggest_semantics(df, dict_df, search_fn=stub_search, include_dwc=True)
        dwc_map = enriched.attrs.get("dwc_mappings")
        self.assertIsNotNone(dwc_map)
        self.assertIsInstance(dwc_map, pd.DataFrame)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
