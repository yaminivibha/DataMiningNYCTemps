"""
Main point of execution.
"""

import argparse
from AssociationRulesExtractor import AssociationRulesExtractor


def main():
    """ """
    # Taking in command line arguments
    parser = argparse.ArgumentParser(
        description="Association Rule Extraction from NYC OpenData datasets"
    )
    parser.add_argument(
        "min_sup",
        type=float,
        help="Minimum support threshold for association rules",
    )
    parser.add_argument(
        "min_conf",
        type=float,
        help="Minimum confidence threshold for association rules",
    )

    args = parser.parse_args()
    extractor = AssociationRulesExtractor(args)
    extractor.print_query_params()
    extractor.run_apriori()
    return


if __name__ == "__main__":
    main()
