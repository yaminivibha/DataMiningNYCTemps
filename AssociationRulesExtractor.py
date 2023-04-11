"""
"""

class AssociationRulesExtractor:
    def __init__(self, args):
        """
        Initialize an AssociationRulesExtractor object.
        Instance variables:
            min_sup: Minimum support threshold for association rules
            min_conf: Minimum confidence threshold for association rules
            freq_itemsets: List of frequent itemsets
        """
        self.min_sup = args.min_sup
        self.min_conf = args.min_conf
        self.dataset = "TODO_CHANGE_THIS_NYC_BUDGET_DATA"
        self.freq_itemsets = [] # Should be a list of dictionaries with the following format:
								# [{itemset: support}, {itemset: support}, ...]
        self.high_conf_rules = []
    
    def computeFrequentItemsets(self):
        pass
    
    def extractAssociationRules(self):
        pass

    def printQueryParams(self):
        """
        Print the query parameters.
        Parameters:
            None
        Returns:
            None
        """
        print("Parameters:")
        print(f"Minimum support         : {self.min_sup}")
        print(f"Minimum confidence      : {self.min_conf}")
        print(f"========================================")
        return
    
    def printResults(self):
        print(f"\n==Frequent itemsets (min_sup={self.min_sup})")
        # TODO: loop through self.freq_itemsets and print each itemset along
        # with its support value
        print(f"==High-confidence association rules (min_conf={self.min_conf})")
        # TODO: loop through self.high_conf_rules and print each rule along
        # with its confidence and support values
        return