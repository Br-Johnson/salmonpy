#!/usr/bin/env python3
"""
Draft a DFO Salmon Ontology new term request from CLI arguments.
Outputs Markdown ready to paste into the GitHub issue template.
"""
import argparse
from textwrap import dedent


def main():
    p = argparse.ArgumentParser(description="Draft a new term request for the DFO Salmon Ontology.")
    p.add_argument("--label", required=True, help="term_label")
    p.add_argument("--definition", required=True, help="term_definition")
    p.add_argument("--term-type", required=True, choices=["skos_concept", "owl_class", "owl_object_property"], help="term_type")
    p.add_argument("--parent-iri", required=True, help="suggested_parent_iri")
    p.add_argument("--definition-source-url", help="definition_source_url")
    p.add_argument("--relationships", help="suggested_relationships (comma-separated)")
    p.add_argument("--notes", help="notes")
    args = p.parse_args()

    md = dedent(
        f"""
        ### Term request
        - term_label: {args.label}
        - term_definition: {args.definition}
        - term_type: {args.term_type}
        - suggested_parent_iri: {args.parent_iri}
        - definition_source_url: {args.definition_source_url or ''}
        - suggested_relationships: {args.relationships or ''}
        - notes: {args.notes or ''}

        Issue template: https://github.com/dfo-pacific-science/dfo-salmon-ontology/issues/new?template=new-term-request.md
        """
    ).strip()
    print(md)


if __name__ == "__main__":
    main()
