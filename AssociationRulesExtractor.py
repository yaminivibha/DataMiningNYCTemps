"""
AssociationRulesExtractor.py
"""
import pandas as pd
from itertools import chain, combinations
from typing import List, Tuple


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
        self.min_support_occurrences = self.min_sup * len(self.df)
        self.freq_itemsets = {} 
        self.high_conf_rules = []
        self.singletons = None
    
    # def data_from_file(self, fname: str) -> frozenset:
    #     """
    #     Function which reads from the file and yields a generator
    #     :params:
    #         - fname: path to the file
    #     :return:
    #         - record: a frozenset of items in the record
    #     """

    #     with open(fname, "r") as iterfile:
    #         for line in iterfile:
    #             line = line.strip().rstrip(",")  # Remove trailing comma
    #             record = frozenset(line.split(","))
    #             yield record
    
    def compute_frequent_itemsets(self):
        """
        Compute frequent itemsets from the dataset.
        :params:
            None
        :return:
            None
        """
        for k in range(len(self.df.columns)):
            self.compute_k_frequent_itemset(k)
        return
    
    def compute_k_frequent_itemset(self, k: int):
        """
        Compute frequent itemsets of size k.
        :params:
            - k: size of the itemset
        :return:
            - None
        """
        if k==1:
            # For each column, count the number of occurences of each value
            for col in self.df.columns:
                # Using value_counts, get the frequency of each item by column
                for key, value in self.df[col].value_counts().to_dict().items():
                    # Add the item to the dictionary if it doesn't exist, otherwise increment the count
                    # by the number of occurences of that item in the current column
                    if key in self.freq_itemsets:
                        self.freq_itemsets[key] += value
                    else:
                        self.freq_itemsets[key] = value
            print(f"Extracted {len(self.freq_itemsets)} items...")
            self.prune_itemsets()
            self.singletons = list(self.freq_itemsets.keys())
        
        else:
            self.apriori_gen(k)
            # Check each row for the presence of candidate itemsets of size k
            for record in self.df.iterrows():
                subsets = combinations(record, k)
                for subset in subsets:
                    if subset in self.freq_itemsets:
                        self.freq_itemsets[subset] += 1
            self.prune_itemsets()
            print(f"Extracted {len(self.freq_itemsets)} items...")
        
        return
    
    def prune_itemsets(self,):
        """
        Prune infrequent itemsets inplace.
        :params:
            None
        :return:
            None
        """
        keys_to_delete = []
        for key, value in self.freq_itemsets.items():
            if value < self.min_support_occurrences:
                keys_to_delete.append(key)
        for key in keys_to_delete:
            del self.freq_itemsets[key]
        print(f"Pruned candidate itemsets... Now {len(self.freq_itemsets)} items...")
        print(self.freq_itemsets)
        return
    
    def apriori_gen(self, k) -> None:
        """
        Generates viable candidate itemsets of size k from frequent itemsets of size k-1.
        Initializes the frequency of viable candidates to 0.
        :params:
            - k: size of the itemset
        :return:
            - None
        """
        viable_candidates = []
        k_tuples = combinations(self.singletons, k)
        
        # Check if all k-1 size subsets of items within k are in self.freq_items
        # Because a large subset is only frequent if all subsets are frequent
        total_candidates = 0
        for candidate in k_tuples:
            total_candidates += 1
            subsets = combinations(candidate, k-1)
            for subset in subsets:
                if subset not in self.freq_itemsets:
                    continue
                viable_candidates.append(candidate)
        print(f"Total {total_candidates} candidate itemsets of size {k}...")
        print(f"{len(viable_candidates)} viable candidate itemsets of size {k}...")
        
        # Initializing frequency of viable candidates to 0 
        for itemset in viable_candidates:
            self.freq_itemsets[itemset] = 0 
        return
        
    def extract_association_rules(self):
        pass

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
        print(f"========================================")
        return
    
    def print_results(self):
        print(f"\n==Frequent itemsets (min_sup={self.min_sup})")
        # TODO: loop through self.freq_itemsets and print each itemset along
        # with its support value
        print(f"==High-confidence association rules (min_conf={self.min_conf})")
        # TODO: loop through self.high_conf_rules and print each rule along
        # with its confidence and support values
        return

    def run_apriori(self):
        """
        Wrapper function for apriori algorithm.
        """
        print(f"\nComputing frequent itemsets from dataset...")
        self.compute_frequent_itemsets()
        # print(f"\nExtracting association rules from dataset...")
