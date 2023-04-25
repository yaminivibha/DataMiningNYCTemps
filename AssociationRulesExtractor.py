"""
AssociationRulesExtractor.py
"""
from itertools import combinations
from pprint import pprint

import pandas as pd
import prettytable as pt


class AssociationRulesExtractor:
    """
    Extracts association rules from a dataset.
    Dataset: INTEGRATED-DATASET.csv
    """

    def __init__(self, args):
        """
        Initialize an AssociationRulesExtractor object.
        Instance variables:
            - min_sup: minimum support threshold
            - min_conf: minimum confidence threshold
            - dataset: path to the dataset
            - df: pandas dataframe of the dataset
            - min_support_occurrences: minimum number of occurrences of an itemset
            - freq_itemsets: dictionary of frequent itemsets
            - high_conf_rules: list of high-confidence association rules
            - singletons: list of high frequency 1-element items
        :params:
            - args: command line arguments
        :return:
            - None
        """
        self.min_sup = args.min_sup
        self.min_conf = args.min_conf
        self.dataset = "INTEGRATED-DATASET.csv"
        self.df = pd.read_csv(self.dataset)
        self.freq_itemsets = {}
        self.high_conf_rules = {}
        self.singletons = set()
        self.data_in_sets = []
        self.put_data_in_sets()

    def put_data_in_sets(self):
        for i in range(len(self.df)):
            self.data_in_sets.append(set(self.df.iloc[i].dropna().values))
        return

    def compute_frequent_itemsets(self, verbose=False):
        """
        Compute frequent itemsets from the dataset.
        :params:
            None
        :return:
            None
        """
        self.compute_k_frequent_itemset(1, verbose=verbose)
        for k in range(2, len(self.df.columns) + 1):
            if self.compute_k_frequent_itemset(k, verbose=verbose) == 0:
                break
        return

    def compute_k_frequent_itemset(self, k: int, verbose: bool = False):
        """
        Compute frequent itemsets of size k.
        :params:
            - k: size of the itemset
        :return:
            - None
        """
        if k == 1:
            self.compute_singleton_frequent_itemset()
            if verbose:
                print("========================================")
                print("k = 1")
                print("Singletons:")
                pprint(self.singletons)
                print(f"Found {len(self.singletons)} singletons achieving min_support")
                print("========================================")
            return
        else:
            if verbose:
                print("========================================\nk = {k}")
            self.generate_candidate_itemsets(k)
            removed = self.prune_itemsets(k)
            if verbose:
                print("========================================")
            return removed

    def generate_candidate_itemsets(self, k: int, verbose: bool = False):
        """
        Function which generates candidate itemsets of size k.
        :params:
            - k: size of the itemset
        :return:
            - None
        """
        combs = combinations(self.singletons, k)
        for comb in combs:
            subsets = combinations(comb, k - 1)
            add = True
            for subset in subsets:
                if subset not in self.freq_itemsets:
                    add = False
                    break
            if add:
                self.freq_itemsets[comb] = 0
        if verbose:
            print(
                f"Now we have {len(self.freq_itemsets)} potential / candidate itemsets of size {k}"
            )
        return combs

    def compute_singleton_frequent_itemset(self):
        """
        Compute frequent itemsets of size 1.
        :params:
            None
        :return:
            None
        """
        # Create a dictionary to store the frequency of each item
        for i in range(len(self.df)):
            for item in self.df.iloc[i].dropna().values:
                if (item,) not in self.freq_itemsets:
                    self.freq_itemsets[(item,)] = 1
                else:
                    self.freq_itemsets[(item,)] += 1

        # Create a set of singletons
        removal = []
        for itemset, count in self.freq_itemsets.items():
            if count >= self.min_sup * len(self.df):
                self.singletons.add(itemset[0])
            else:
                removal.append(itemset)

        # Remove items that do not meet the minimum support threshold
        for item in removal:
            del self.freq_itemsets[item]
        return

    def prune_itemsets(self, k: int, verbose: bool = False) -> int:
        """
        Remove itemsets that do not meet the minimum support threshold.
        :params:
            - k (int): size of the itemset
        :return:
            - removed (int): number of itemsets removed
        """
        for row in self.data_in_sets:
            for key in self.freq_itemsets.keys():
                if len(key) == k:
                    key_set = set(key)
                    if key_set.issubset(row):
                        self.freq_itemsets[key] += 1
        removal = []
        for itemset, count in self.freq_itemsets.items():
            if count < self.min_sup * len(self.df):
                removal.append(itemset)
        for item in removal:
            del self.freq_itemsets[item]
        if verbose:
            print(
                f"Removed {len(removal)} itemsets of size {k} that did not meet the minimum support threshold"
            )
        return len(removal)

    def extract_association_rules(self):
        """
        Iterate over each frequent itemset
            generate all k-1 subsets
                for each subset: rule is (k-1 subset) -> (itemset - subset):
                    check if rule meets minimum confidence threshold (if so, add to high_conf_rules)
                else: prune rule
        :params:
            None
        :return:
            None
        """
        for itemset in self.freq_itemsets.keys():
            if len(itemset) > 1:
                subsets = combinations(itemset, len(itemset) - 1)
                for subset in subsets:
                    rule = (subset, tuple(set(itemset) - set(subset)))
                    passes_threshold, confidence = self.check_confidence(rule)
                    if passes_threshold:
                        self.high_conf_rules[rule] = confidence

    def check_confidence(self, rule: tuple) -> bool:
        """
        Check if a rule meets the minimum confidence threshold.
        :params:
            - rule (tuple): rule to check
        :return:
            - bool: True if rule meets minimum confidence threshold, False otherwise
        """
        itemset = rule[0]
        subset = rule[1]
        confidence = self.freq_itemsets[itemset] / self.freq_itemsets[subset]
        if confidence >= self.min_conf:
            return True, confidence
        else:
            return False, confidence
        
    def print_query_params(self):
        """
        Print the query parameters.
        :params:
            None
        :return:
            None
        """
        print("Parameters:")
        print(f"Minimum support         : {self.min_sup}")
        print(f"Minimum confidence      : {self.min_conf}")
        print("========================================")
        return

    def print_itemsets(self):
        """
        Print frequent itemsets and their support values in a table.
        :params:
            None
        :return:
            None
        """
        # Create table containing frequent itemsets and support values
        table = pt.PrettyTable()
        table.title = f"Frequent itemsets (min_sup={self.min_sup})"
        table.field_names = ["Itemset", "Support %"]
        table.float_format = "0.4"
        for itemset, count in self.freq_itemsets.items():
            # itemset_output = ", ".join(itemset)
            table.add_row([itemset, f"{count / len(self.df) * 100} %"])
        # TODO: sort table by support (descending)
        print(table)
        print("Total number of frequent itemsets: ", len(self.freq_itemsets))
        print("========================================")

    def print_rules(self):
        """
        Create a table containing strong association rules and confidence values.
        :params:
            None
        :return:
            None
        """
        # Create table containing strong association rules and confidence values
        table = pt.PrettyTable()
        table.title = f"Strong Association Rules (min_conf={self.min_conf})"
        table.field_names = ["Rule", "Confidence"]
        for rule, conf in self.high_conf_rules.items():
            table.add_row([f"{rule[0]} => {rule[1]}", f"{ conf * 100} %"])
        print(table)

    def run_apriori(self):
        """
        Wrapper function for apriori algorithm.
        :params:
            None
        :return:
            None
        """
        # Check if frequent_itemsets.pkl exists
        # if os.path.exists("frequent_itemsets.pkl"):
        #     self.unpickle_freq_itemsets()
        # else:
        print("\nComputing frequent itemsets from dataset...")
        self.compute_frequent_itemsets()
        self.print_itemsets()

        print("\nExtracting association rules from dataset...")
        self.extract_association_rules()
        self.print_rules()
