from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import urllib.parse
import urllib.request
import warnings
from typing import Dict, Iterable, List, Optional, Sequence

try:
    import pandas as pd
except ImportError as exc:  # pragma: no cover - import guard
    raise ImportError("salmonpy requires pandas; install via `pip install pandas`.") from exc

try:
    from importlib import resources
except ImportError as exc:  # pragma: no cover
    raise

try:  # pragma: no cover - best effort metadata lookup
    from importlib.metadata import version as _pkg_version
except ImportError:  # pragma: no cover
    _pkg_version = None


_warned_bioportal_missing = False
_cache_enabled = os.getenv("SALMONPY_CACHE", "").lower() in {"1", "true", "yes"}
_term_cache: Dict[tuple, pd.DataFrame] = {}
_USER_AGENT = "salmonpy/unknown"
if _pkg_version:
    try:
        _USER_AGENT = f"salmonpy/{_pkg_version('salmonpy')}"
    except Exception:  # pragma: no cover - fallback
        _USER_AGENT = "salmonpy/unknown"


def _empty_terms(role=None) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "label": pd.Series(dtype=object),
            "iri": pd.Series(dtype=object),
            "source": pd.Series(dtype=object),
            "ontology": pd.Series(dtype=object),
            "role": pd.Series(dtype=object),
            "match_type": pd.Series(dtype=object),
            "definition": pd.Series(dtype=object),
            "alignment_only": pd.Series(dtype=bool),
        }
    )


def _safe_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> Optional[dict]:
    merged_headers = headers.copy() if headers else {}
    merged_headers.setdefault("User-Agent", _USER_AGENT)

    _debug = os.getenv("SALMONPY_DEBUG_FETCH", "").lower() in {"1", "true", "yes"}

    try:
        req = urllib.request.Request(url, headers=merged_headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status >= 300:
                if _debug:
                    print(f"[_safe_json] HTTP error {resp.status}", file=__import__('sys').stderr)
                return None
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except Exception as _urllib_err:
        # Some environments ship Python builds that fail to establish HTTPS
        # connections (e.g., Errno 8/9 "nodename nor servname provided" or
        # "Bad file descriptor"). Fall back to curl if available.
        if _debug:
            print(f"[_safe_json] urllib failed: {type(_urllib_err).__name__}", file=__import__('sys').stderr)
        try:
            if shutil.which("curl") is None:
                if _debug:
                    print("[_safe_json] curl not found", file=__import__('sys').stderr)
                return None
            cmd = ["curl", "-s", "-L", url]
            for key, value in merged_headers.items():
                cmd.extend(["-H", f"{key}: {value}"])
            if _debug:
                print(f"[_safe_json] running curl (timeout={timeout})...", file=__import__('sys').stderr)
            body = subprocess.check_output(cmd, timeout=timeout).decode("utf-8")
            if _debug:
                print(f"[_safe_json] curl success: {len(body)} bytes", file=__import__('sys').stderr)
            return json.loads(body) if body else None
        except Exception as _curl_err:
            if _debug:
                print(f"[_safe_json] curl failed: {type(_curl_err).__name__}: {_curl_err}", file=__import__('sys').stderr)
            return None


def _load_iadopt_vocab() -> pd.DataFrame:
    try:
        vocab_path = resources.files("salmonpy").joinpath("data/iadopt-terminologies.csv")
        with resources.as_file(vocab_path) as path:
            df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

    df["host"] = df["ttl_url"].apply(lambda u: urllib.parse.urlparse(u).hostname or "")
    df["slug"] = df["ttl_url"].apply(lambda u: os.path.splitext(os.path.basename(u))[0])
    df["label_tokens"] = df["label"].apply(lambda x: re.sub(r"[^a-z0-9]+", " ", str(x).lower()))
    return df


def _load_role_preferences() -> pd.DataFrame:
    try:
        pref_path = resources.files("salmonpy").joinpath("data/ontology-preferences.csv")
        with resources.as_file(pref_path) as path:
            df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame(
            columns=[
                "role",
                "ontology",
                "priority",
                "source_hint",
                "iri_pattern",
                "alignment_only",
                "notes",
            ]
        )

    if "alignment_only" in df.columns:
        df["alignment_only"] = df["alignment_only"].astype(str).str.lower().isin({"true", "1", "yes"})
    return df


def _search_ols(query: str, role) -> pd.DataFrame:
    encoded = urllib.parse.quote(query, safe="")
    url = f"https://www.ebi.ac.uk/ols4/api/search?q={encoded}&rows=50"
    data = _safe_json(url)
    docs = data.get("response", {}).get("docs", []) if data else []
    if not docs:
        return _empty_terms(role)

    docs_df = pd.DataFrame(docs)
    desc_series = docs_df.get("description", pd.Series([], dtype=object)).apply(
        lambda x: x[0] if isinstance(x, list) and x else ""
    )
    return pd.DataFrame(
        {
            "label": docs_df.get("label", pd.Series([], dtype=object)).fillna(""),
            "iri": docs_df.get("iri", pd.Series([], dtype=object)).fillna(""),
            "source": "ols",
            "ontology": docs_df.get("ontology_name", pd.Series([], dtype=object)).fillna(""),
            "role": role,
            "match_type": docs_df.get("type", pd.Series([], dtype=object)).fillna(""),
            "definition": desc_series,
        }
    )


def _search_nvs(query: str, role) -> pd.DataFrame:
    tokens = list({tok for tok in re.sub(r"[^a-z0-9]+", " ", str(query).lower()).split() if tok})
    if not tokens:
        return _empty_terms(role)

    # NVS search_nvs endpoints are not reliable; use the SPARQL endpoint instead.
    # Restrict to P01 (observables) and P06 (units).
    # Use simple CONTAINS on prefLabel for speed (REGEX + OPTIONAL is too slow on P01).
    pattern = ".*".join(tokens)
    sparql = (
        "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n"
        "SELECT DISTINCT ?uri ?label ?definition WHERE {\n"
        "  ?uri skos:prefLabel ?label .\n"
        "  OPTIONAL { ?uri skos:definition ?definition . }\n"
        "  FILTER(\n"
        "    STRSTARTS(STR(?uri), \"http://vocab.nerc.ac.uk/collection/P01/\") ||\n"
        "    STRSTARTS(STR(?uri), \"http://vocab.nerc.ac.uk/collection/P06/\")\n"
        "  )\n"
        f"  FILTER(REGEX(LCASE(STR(?label)), \"{pattern}\"))\n"
        "}\n"
        "LIMIT 50\n"
    )

    url = "https://vocab.nerc.ac.uk/sparql/?" + urllib.parse.urlencode({"query": sparql})
    data = _safe_json(url, headers={"Accept": "application/sparql-results+json"}, timeout=60)
    bindings = data.get("results", {}).get("bindings", []) if data else []
    if not bindings:
        return _empty_terms(role)

    rows = []
    for b in bindings:
        iri = b.get("uri", {}).get("value", "")
        label = b.get("label", {}).get("value", "")
        definition = b.get("definition", {}).get("value", "") if isinstance(b.get("definition"), dict) else ""
        match = re.match(r"^http://vocab\.nerc\.ac\.uk/collection/([^/]+)/", iri)
        ontology = match.group(1) if match else ""
        rows.append(
            {
                "label": label,
                "iri": iri,
                "source": "nvs",
                "ontology": ontology,
                "role": role,
                "match_type": "concept",
                "definition": definition,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return _empty_terms(role)
    return df.drop_duplicates(subset=["iri"]).reset_index(drop=True)


def _search_zooma(query: str, role) -> pd.DataFrame:
    encoded = urllib.parse.quote(query, safe="")
    url = f"https://www.ebi.ac.uk/spot/zooma/v2/api/services/annotate?propertyValue={encoded}"
    data = _safe_json(url, headers={"Accept": "application/json"}, timeout=60)
    if not isinstance(data, list) or not data:
        return _empty_terms(role)

    hrefs: List[str] = []
    confidence_by_iri: Dict[str, str] = {}
    for ann in data:
        if not isinstance(ann, dict):
            continue
        conf = str(ann.get("confidence") or "")
        for tag in ann.get("semanticTags", []) or []:
            if isinstance(tag, str) and tag and tag not in confidence_by_iri:
                confidence_by_iri[tag] = conf
        links = (ann.get("_links") or {}).get("olslinks", []) or []
        for link in links:
            if not isinstance(link, dict):
                continue
            href = link.get("href")
            if isinstance(href, str) and href and href not in hrefs:
                hrefs.append(href)

    hrefs = hrefs[:25]
    rows = []
    for href in hrefs:
        term_data = _safe_json(href)
        terms = (term_data.get("_embedded") or {}).get("terms", []) if isinstance(term_data, dict) else []
        if not terms:
            continue
        term = terms[0] if isinstance(terms[0], dict) else {}
        iri = str(term.get("iri") or "")
        label = str(term.get("label") or "")
        ontology = str(term.get("ontology_name") or "")
        desc = term.get("description") or []
        definition = desc[0] if isinstance(desc, list) and desc else ""
        confidence = confidence_by_iri.get(iri) or "unknown"
        match_type = f"zooma_{confidence.lower()}" if confidence else "zooma_unknown"
        rows.append(
            {
                "label": label,
                "iri": iri,
                "source": "zooma",
                "ontology": ontology,
                "role": role,
                "match_type": match_type,
                "definition": definition,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return _empty_terms(role)
    return df.drop_duplicates(subset=["iri"]).reset_index(drop=True)


def _search_bioportal(query: str, role) -> pd.DataFrame:
    apikey = os.getenv("BIOPORTAL_APIKEY", "")
    if not apikey:
        global _warned_bioportal_missing
        if not _warned_bioportal_missing:
            warnings.warn(
                "BioPortal API key missing; set env BIOPORTAL_APIKEY and restart your session. "
                "Example (bash/zsh): export BIOPORTAL_APIKEY=your_key_here "
                "Persist it in ~/.Renviron or ~/.zshrc with a line: BIOPORTAL_APIKEY=your_key_here "
                "Get a key at https://bioportal.bioontology.org/register. "
                "Do not paste keys into chat; keep them in your environment.",
                RuntimeWarning,
            )
            _warned_bioportal_missing = True
        return _empty_terms(role)
    encoded = urllib.parse.quote(query, safe="")
    url = f"https://data.bioontology.org/search?q={encoded}&apikey={apikey}"
    data = _safe_json(url)
    coll = data.get("collection", []) if data else []
    if not coll:
        return _empty_terms(role)

    coll_df = pd.DataFrame(coll)
    ontology_series = coll_df.get("links", pd.Series([], dtype=object)).apply(
        lambda x: x.get("ontology") if isinstance(x, dict) else ""
    )

    return pd.DataFrame(
        {
            "label": coll_df.get("prefLabel", pd.Series([], dtype=object)).fillna(""),
            "iri": coll_df.get("@id", pd.Series([], dtype=object)).fillna(""),
            "source": "bioportal",
            "ontology": ontology_series.fillna(""),
            "role": role,
            "match_type": coll_df.get("matchType", pd.Series([], dtype=object)).fillna(""),
            "definition": [
                (desc[0] if isinstance(desc, list) and desc else "")
                for desc in coll_df.get("definition", pd.Series([], dtype=object))
            ],
        }
    )


def _search_qudt(query: str, role) -> pd.DataFrame:
    tokens = list({tok for tok in re.sub(r"[^a-z0-9]+", " ", str(query).lower()).split() if tok})
    if not tokens:
        return _empty_terms(role)

    pattern = ".*".join(tokens)
    sparql = (
        "PREFIX qudt: <http://qudt.org/schema/qudt/>\n"
        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
        "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n"
        "SELECT DISTINCT ?uri ?label ?definition WHERE {\n"
        "  ?uri a qudt:Unit .\n"
        "  ?uri rdfs:label ?label .\n"
        "  OPTIONAL { ?uri skos:definition ?definition . }\n"
        "  OPTIONAL { ?uri qudt:description ?definition . }\n"
        f"  FILTER(REGEX(LCASE(STR(?label)), \"{pattern}\", \"i\"))\n"
        "}\n"
        "LIMIT 50\n"
    )
    url = "https://www.qudt.org/fuseki/qudt/sparql?" + urllib.parse.urlencode({"query": sparql})
    data = _safe_json(url, headers={"Accept": "application/sparql-results+json"}, timeout=60)
    bindings = data.get("results", {}).get("bindings", []) if data else []
    if not bindings:
        return _empty_terms(role)

    rows = []
    for b in bindings:
        iri = b.get("uri", {}).get("value", "")
        label = b.get("label", {}).get("value", "")
        definition = b.get("definition", {}).get("value", "") if isinstance(b.get("definition"), dict) else ""
        rows.append(
            {
                "label": label,
                "iri": iri,
                "source": "qudt",
                "ontology": "qudt",
                "role": role,
                "match_type": "unit",
                "definition": definition,
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return _empty_terms(role)
    return df.drop_duplicates(subset=["iri"]).reset_index(drop=True)


def _search_gbif(query: str, role) -> pd.DataFrame:
    encoded = urllib.parse.quote(query, safe="")
    url = f"https://api.gbif.org/v1/species/match?name={encoded}&verbose=true"
    data = _safe_json(url, timeout=30)
    if not data or not data.get("usageKey"):
        url = f"https://api.gbif.org/v1/species/search?q={encoded}&limit=20"
        data = _safe_json(url, timeout=30)
        results = data.get("results", []) if data else []
        if not results:
            return _empty_terms(role)
        rows = []
        for item in results:
            label = item.get("scientificName") or item.get("canonicalName") or ""
            key = item.get("key")
            if not key:
                continue
            rows.append(
                {
                    "label": label,
                    "iri": f"https://www.gbif.org/species/{key}",
                    "source": "gbif",
                    "ontology": "gbif_backbone",
                    "role": role,
                    "match_type": str(item.get("rank") or "taxon").lower(),
                    "definition": "; ".join(
                        part
                        for part in [
                            f"Kingdom: {item.get('kingdom')}" if item.get("kingdom") else None,
                            f"Phylum: {item.get('phylum')}" if item.get("phylum") else None,
                            f"Class: {item.get('class')}" if item.get("class") else None,
                            f"Order: {item.get('order')}" if item.get("order") else None,
                            f"Family: {item.get('family')}" if item.get("family") else None,
                        ]
                        if part
                    ),
                }
            )
        df = pd.DataFrame(rows)
        if df.empty:
            return _empty_terms(role)
        return df.drop_duplicates(subset=["iri"]).reset_index(drop=True)

    label = data.get("scientificName") or data.get("canonicalName") or ""
    key = data.get("usageKey")
    if not key:
        return _empty_terms(role)
    return pd.DataFrame(
        {
            "label": [label],
            "iri": [f"https://www.gbif.org/species/{key}"],
            "source": ["gbif"],
            "ontology": ["gbif_backbone"],
            "role": [role],
            "match_type": [str(data.get("rank") or "taxon").lower()],
            "definition": [
                "; ".join(
                    part
                    for part in [
                        f"Kingdom: {data.get('kingdom')}" if data.get("kingdom") else None,
                        f"Phylum: {data.get('phylum')}" if data.get("phylum") else None,
                        f"Class: {data.get('class')}" if data.get("class") else None,
                        f"Order: {data.get('order')}" if data.get("order") else None,
                        f"Family: {data.get('family')}" if data.get("family") else None,
                    ]
                    if part
                )
            ],
        }
    )


def _search_worms(query: str, role) -> pd.DataFrame:
    encoded = urllib.parse.quote(query, safe="")
    url = (
        "https://www.marinespecies.org/rest/AphiaRecordsByName/"
        f"{encoded}?like=true&marine_only=false&offset=1"
    )
    data = _safe_json(url, timeout=30)
    if not isinstance(data, list) or not data:
        url = (
            "https://www.marinespecies.org/rest/AphiaRecordsByMatchNames"
            f"?scientificnames%5B%5D={encoded}"
        )
        data = _safe_json(url, timeout=30)
        if not data or not isinstance(data, list) or not data[0]:
            return _empty_terms(role)
        data = data[0]

    rows = []
    for item in data:
        if not isinstance(item, dict):
            continue
        aphia_id = item.get("AphiaID")
        if not aphia_id:
            continue
        rows.append(
            {
                "label": item.get("scientificname") or "",
                "iri": f"urn:lsid:marinespecies.org:taxname:{aphia_id}",
                "source": "worms",
                "ontology": "worms",
                "role": role,
                "match_type": str(item.get("rank") or "taxon").lower(),
                "definition": "; ".join(
                    part
                    for part in [
                        f"Kingdom: {item.get('kingdom')}" if item.get("kingdom") else None,
                        f"Phylum: {item.get('phylum')}" if item.get("phylum") else None,
                        f"Class: {item.get('class')}" if item.get("class") else None,
                        f"Order: {item.get('order')}" if item.get("order") else None,
                        f"Family: {item.get('family')}" if item.get("family") else None,
                    ]
                    if part
                ),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return _empty_terms(role)
    return df.drop_duplicates(subset=["iri"]).reset_index(drop=True)


def _score_and_rank_terms(df: pd.DataFrame, role, vocab_tbl: pd.DataFrame, query: Optional[str] = None) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    role_prefs = _load_role_preferences()
    base_source_weight = {
        "ols": 0.3,
        "nvs": 0.6,
        "zooma": 0.5,
        "bioportal": 0.2,
        "qudt": 0.7,
        "gbif": 0.6,
        "worms": 0.6,
    }
    role_boost = {
        "unit": {"qudt": 1.5, "nvs": 1.2, "ols": 0.3},
        "property": {"nvs": 1.0, "ols": 0.4},
        "variable": {"nvs": 0.6, "ols": 0.2, "bioportal": 0.4},
        "entity": {"gbif": 1.3, "worms": 1.3, "bioportal": 0.4, "ols": 0.4},
        "constraint": {"ols": 0.4, "bioportal": 0.4},
        "method": {"bioportal": 0.4, "ols": 0.4},
    }

    df["score"] = df["source"].map(base_source_weight).fillna(0)

    query_tokens: List[str] = []
    if query:
        query_tokens = list({tok for tok in re.sub(r"[^a-z0-9]+", " ", query.lower()).split() if tok})

    role_key = role if role is not None else None
    if role_key and role_key in role_boost:
        df["score"] += df["source"].map(role_boost.get(role_key, {})).fillna(0)

    if not role_prefs.empty and role_key:
        role_specific = role_prefs[(role_prefs["role"] == role_key) | (role_prefs["role"] == "wikidata")]
        alignment_flags: List[bool] = [False] * len(df)
        boosts: List[float] = [0.0] * len(df)
        for idx, iri in enumerate(df["iri"].fillna("").astype(str)):
            for _, pref in role_specific.iterrows():
                pattern = str(pref.get("iri_pattern") or "")
                if pattern and re.search(pattern, iri, re.IGNORECASE):
                    if bool(pref.get("alignment_only")):
                        boosts[idx] = -0.5
                        alignment_flags[idx] = True
                    else:
                        try:
                            priority = float(pref.get("priority"))
                        except (TypeError, ValueError):
                            priority = None
                        boost = max(0, 2.5 - (priority * 0.5)) if priority is not None else 0
                        boosts[idx] = boost
                    break
        df["score"] += pd.Series(boosts, index=df.index)
        df["alignment_only"] = alignment_flags

    role_vocabs = vocab_tbl[vocab_tbl["role"] == role_key] if (role_key and not vocab_tbl.empty) else pd.DataFrame()
    if not role_vocabs.empty:
        host_pattern = "|".join(role_vocabs["host"].dropna().unique())
        slug_pattern = "|".join(role_vocabs["slug"].dropna().unique())
        label_pattern = "|".join(role_vocabs["label_tokens"].dropna().unique())

        if host_pattern:
            df.loc[df["iri"].str.contains(host_pattern, case=False, na=False), "score"] += 1
        if slug_pattern:
            df.loc[
                df["iri"].str.contains(slug_pattern, case=False, na=False)
                | df["ontology"].str.contains(slug_pattern, case=False, na=False),
                "score",
            ] += 1
        if label_pattern:
            df.loc[df["ontology"].str.contains(label_pattern, case=False, na=False), "score"] += 0.5

    if query_tokens:
        def _label_overlap(lbl: str) -> float:
            lbl_tokens = {tok for tok in re.sub(r"[^a-z0-9]+", " ", str(lbl).lower()).split() if tok}
            return len(lbl_tokens.intersection(query_tokens)) * 0.2

        df["score"] += df["label"].apply(_label_overlap)

    if "alignment_only" not in df.columns:
        df["alignment_only"] = df["iri"].str.contains("wikidata.org", case=False, na=False)
    else:
        df["alignment_only"] = df["alignment_only"] | df["iri"].str.contains("wikidata.org", case=False, na=False)

    return df.sort_values(by=["score", "source", "ontology", "label", "iri"], ascending=[False, True, True, True, True])


def sources_for_role(role: Optional[str]) -> List[str]:
    if role is None or role == "":
        return ["ols", "nvs"]

    role_key = str(role).lower()
    if role_key == "unit":
        return ["qudt", "nvs", "ols"]
    if role_key == "property":
        return ["nvs", "ols", "zooma"]
    if role_key == "entity":
        return ["gbif", "worms", "bioportal", "ols"]
    if role_key == "method":
        return ["bioportal", "ols", "zooma"]
    if role_key == "variable":
        return ["nvs", "ols", "zooma"]
    if role_key == "constraint":
        return ["ols"]
    return ["ols", "nvs"]


def find_terms(query: str, role: Optional[str] = None, sources: Sequence[str] = ("ols", "nvs")) -> pd.DataFrame:
    """
    Find ontology terms across OLS, NVS, and other vocab sources.
    """
    if not sources or query is None or query == "":
        return _empty_terms(role)

    cache_key = (query, role, tuple(sorted(sources)))
    if _cache_enabled and cache_key in _term_cache:
        return _term_cache[cache_key].copy()

    results = []
    for src in sources:
        if src == "ols":
            results.append(_search_ols(query, role))
        elif src == "nvs":
            results.append(_search_nvs(query, role))
        elif src == "zooma":
            results.append(_search_zooma(query, role))
        elif src == "bioportal":
            results.append(_search_bioportal(query, role))
        elif src == "qudt":
            results.append(_search_qudt(query, role))
        elif src == "gbif":
            results.append(_search_gbif(query, role))
        elif src == "worms":
            results.append(_search_worms(query, role))
        else:
            results.append(_empty_terms(role))

    combined = pd.concat(results, ignore_index=True) if results else _empty_terms(role)
    vocab_tbl = _load_iadopt_vocab()
    ranked = _score_and_rank_terms(combined, role, vocab_tbl, query)
    if "alignment_only" not in ranked.columns:
        ranked["alignment_only"] = False
    ranked = ranked[["label", "iri", "source", "ontology", "role", "match_type", "definition", "alignment_only"]]
    if _cache_enabled:
        _term_cache[cache_key] = ranked.copy()
    return ranked


__all__ = ["find_terms", "sources_for_role"]
