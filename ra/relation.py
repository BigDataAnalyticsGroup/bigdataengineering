import pandas as pd
import math
import bisect
import operator


##################
# Schema Parsing #
##################

def build_schema(attributes, domains):
    """Builds the schema needed to initialize a new relation object

    Args:
        attributes(`list` of :obj: `string`): Names of the attributes in the schema, ordered.
        domains (`list` of :obj: `type`): Types of the attributes in the schema, ordered.

    Returns:
        schema (`list` of `tuple` of :obj: `string` and :obj: `type): Schema of a relation.
    """
    # integrity checks
    assert len(attributes) == len(domains)  # Length of attributes and domains should match
    assert all(map(lambda x: isinstance(x, str), attributes))  # All attributes need to be strings
    assert all(map(lambda x: isinstance(x, type), domains))  # All domains need to be types
    # build attribute list
    return [*map(lambda x: (x[0], x[1]), zip(attributes, domains))]

def parse_schema(schema):
    """Builds attributes and domains tuples from schema

    Args:
        schema (`list` of `tuple` of :obj: `string` and :obj: `type): Schema of a relation.

    Returns:
        attributes(`list` of :obj: `string`): Names of the attributes in the schema, ordered.
        domains (`list` of :obj: `type`): Types of the attributes in the schema, ordered.
    """
    # split schema into attributes and domains
    attributes, domains = tuple(zip(*schema))
    # check that attribute names and domain types are valid
    assert all(map(lambda x: x.isidentifier(), attributes))  # all elements of attributes should be an identifier
    assert len(set(attributes)) == len(attributes)  # each attribute name should be unique
    assert all(map(lambda x: isinstance(x, type), domains))  # all elements of domains should be types
    # return attributes and domains
    return attributes, domains


############
# Relation #
############

class Relation:
    """
    Implementation of a relation with name, schema, and set of tuples

    Attributes:
        name (:obj: `string`): Name of the relation.
        attributes(`list` of :obj: `string`): Names of the attributes in the relation, ordered.
        domains (`list` of :obj: `type`): Types of the attributes in the relation, ordered.
        tuples (`set` of `tuple`): Set of tuples in the relation, unordered, without duplicates.
        str_to_type (`dict` of :obj: `string` to :obj: `type`): Maps `string` representation of type to type objects.
        type_to_str (`dict` of :obj: `type` to :obj: `string`): Maps `type` object to string representation.
    """

    str_to_type = {'int': int, 'float': float, 'str': str}
    type_to_str = {int: 'int', float: 'float', str: 'str'}

    def __init__(self, name, schema):
        """
        Instantiates an object of the Relation class

        Args:
            name (:obj: `string): Name of the relation or expression the relation object is built from.
            schema (`list` of `tuple` of :obj: `string` and :obj: `type): Schema of the relation.
        """
        # derive attributes and domains from schema
        attributes, domains = parse_schema(schema)
        # set class variables
        self.name = name  # name of the relation or expression the relation object was built from
        self.attributes = attributes  # list of attribute names
        self.domains = domains  # list of attribute types
        self.tuples = set()  # this ensures not having duplicates
        self.indexes = dict() # stores all secondary indexes of this relation, referenced by attribute

    def add_tuple(self, tup):
        """
        Adds the tuple tup to the relation.

        Args:
             tup (`tuple`): tuple to be added to the relation.

        Returns:
            True if `tup` was added and False if `tup` was already contained
        """
        # check if tuple matches the schema
        self._check_schema(tup)
        # add tuple
        if tup not in self.tuples:
            self.tuples.add(tup)
            return True  # tuple was added
        else:
            return False  # tuple already existed


    def build_index(self, attribute):
        """
        Build a secondary index on the specified attribute

        Args:
            attribute (:obj: `string`): the attribute name to be tested

        """
        assert(self.has_attribute(attribute))

        # create new index
        new_index = dict()
        attribute_index = self.get_attribute_index(attribute)
        for t in self.tuples:
            new_index[t[attribute_index]] = t
        self.indexes[attribute] = new_index

    def has_index_on(self, attribute):
        assert(self.has_attribute(attribute))
        return attribute in self.indexes

    def get_index_on(self, attribute):
        assert(self.has_attribute(attribute))
        if(self.has_index_on(attribute)):
            return self.indexes[attribute]
        else:
            return None

    def _check_schema(self, tup):
        """
        Performs assertions to check wether `tup` matches the relation schema.

        Args:
            tup (`tuple`): The tuple to be tested.
        """
        assert isinstance(tup, tuple)  # tuple should be of type tuple
        assert len(tup) == len(self.domains)  # tuple should have correct amount of attributes
        assert all(map(lambda pair: isinstance(*pair), zip(tup, self.domains)))  # types of all attributes must match relation

    def set_name(self, name):
        """
        Changes the name of the relation object to the name provided

        Args:
            name (:obj: string`): the new name for the relation.
        """
        self.name = name

    def print_schema(self):
        """
        Prints the schema of the relation
        """
        # here string representation of a relation is its schema
        print(str(self))

    def print_table(self, limit=None, _print=True):
        """
        Prints the relation and its tuples in a tabular layout
        """
        assert (limit is None or limit>0)

        # calculate column width for printing
        col_width = self._get_col_width()
        # relation name bold
        target = '-'*len(self.name) + '\n' \
                 '\033[1m' + self.name + '\033[0m \n'
        # attribute names bold
        target += '-'*max(len(self.name), col_width*len(self.attributes)) + '\n' \
                  '\033[1m' + ''.join(attr_name.ljust(col_width) for attr_name in self.attributes) + '\033[0m \n'
        target += '-'*max(len(self.name), col_width*len(self.attributes)) + '\n'
        # tuples
        limitCount = 0
        for tup in self.tuples:
            target += ''.join(str(attr_val).ljust(col_width) for attr_val in tup) + '\n'
            limitCount += 1
            if limit != None and limitCount >= limit:
                target += "\nWARNING: skipping " + str(len(self.tuples)-limit) + " out of " + str(len(self.tuples)) + " tuples..."
                break
        if _print:
            print(target)
        else:
            return target


    def print_set(self, limit=None, _print=True):
        """
        Prints the relation and its tuples in set notation
        """
        assert (limit is None or limit>0)
        target = str(self) + '\n{\n'
        limitCount = 0
        skip = False
        for tup in self.tuples:
            target += '\t(' + ', '.join(str(attr) for attr in tup) + '),\n'
            limitCount += 1
            if limit != None and limitCount >= limit:
                skip = True
                break
        target = target.rstrip("\n").rstrip(",")
        if skip:
            target += "\n\tWARNING: skipping " + str(len(self.tuples)-limit) + " out of " + str(len(self.tuples)) + " tuples..."
        target += '\n}'
        if _print:
            print(target)
        else:
            return target

    def print_latex(self):
        """
        Prints LaTeX code for the relation in tabular layout
        """
        num_cols = len(self.attributes)
        # the following requires \usepackage{xcolor,colortbl} in LaTeX
        latex = '\definecolor{tableheadercolor}{rgb}{0.8,0.8,0.8}'\
                '\\begin{tabular}{|'+('l|'*num_cols)+'}\\hline\n'\
                '\\multicolumn{'+str(num_cols)+'}{|l|}{\\cellcolor{tableheadercolor}{\\textbf{'+Relation._escape_latex_symbols(self.name) +'}}}\\\\\\hline\n'\
                + '\t'+' & '.join("\\cellcolor{tableheadercolor}{\\textbf{"+Relation._escape_latex_symbols(attr)+"}}"\
                                  for attr in self.attributes)+' \\\\\n' \
                + '\t\\hline\\hline\n'
        for tup in self.tuples:
            latex += '\t'+' & '.join(str(attr) for attr in tup)+' \\\\\n'
        latex += '\\hline\n\\end{tabular}'
        print(latex)

    @staticmethod
    def _escape_latex_symbols(string):
        """
        Replaces reserved symbols with escaped version for LaTeX output

        Args:
            string (:obj: `string`): the string to be modified

        Returns:
            LaTeX friendly string
        """
        sym_map = {'#': '\\# ', '$': '\\textdollar ', '%': '\\percent ', '&': '\\& ', '\\': '\\textbackslash ',\
                   '^': '\textcircumflex ', '_': '\\textunderscore ', '{': '\\textbraceleft ', \
                   '|': '\\textbar ', '}': '\\textbraceright ', '~': '\\textasciitilde '}
        for sym in sym_map.keys():
            string = string.replace(sym, sym_map[sym])
        return string


    def to_DataFrame(self):
        """
        Converts the relation into a pandas DataFrame

        Returns:
            `pandas.DataFrame` representation of the relation
        """
        df = pd.DataFrame(list(self.tuples), columns=self.attributes)
        return df


    def _get_col_width(self):
        """
        Computes the maximum column width required to represent the relation in tabular layout.

        Returns:
            The maximum column width required.
        """
        attr_name_width = max(len(attr_name) for attr_name in self.attributes)
        attr_val_width = max((len(str(attr_val)) for tup in self.tuples for attr_val in tup), default=0)
        return max(attr_name_width, attr_val_width) + 2  # padding

    def has_attribute(self, attribute):
        """
        Determines if the relation has a given attribute

        Args:
            attribute (:obj: `string`): the attribute name to be tested

        Returns:
            True if the relation has a attibute with the given name, false otherwise
        """
        return attribute in self.attributes

    def get_attribute_domain(self, attribute):
        """
        Determines the domain of a given attribute

        Args:
            attribute (:obj: `string`): the attribute name to be tested

        Returns:
            the domain of the given attribute
        """
        # integrity checks
        assert self.has_attribute(attribute)  # relation should have the attribute
        # return attr domain
        return self.domains[self.attributes.index(attribute)]

    def get_attribute_index(self, attribute):
        """
        Determines the position of a given attribute in the tuples of a relation

        Args:
            attribute (:obj: `string`): the attribute name to be tested

        Returns:
            the position of the attribute in a tuple of the relation
        """
        # integrity checks
        assert self.has_attribute(attribute)  # relation should have the attribute
        # return index
        return self.attributes.index(attribute)

    def __str__(self):
        """
        Computes a string representation of the relation object, here the schema

        Returns:
            string representation of the object
        """
        rel = f'[{self.name}]'
        attrs = ', '.join([f'{self.attributes[i]}:{Relation.type_to_str[self.domains[i]]}' for i in range(len(self.attributes))])
        return f'{rel} : {{[{attrs}]}}'


    def __repr__(self):
        return self.print_table(limit=10, _print=False)

    def __len__(self):
        """
        Computes the amount of tuples in the given relation

        Returns:
            the amount of tuples contained in the relation
        """
        return len(self.tuples)

    def __eq__(self, other):
        """
        Computes whether two relations are equal, i.e. they contain the same tuples

        Note: This does not consider attribute names.

        Args:
            other (:obj: `Relation`): the relation to compare to self

        Returns:
            True if the relations are equal
        """
        return self.tuples == other.tuples


class Index:
    def sort_by_key(self, t):
        return t[0]

    def __init__(self, relation, attribute):
        self.relation = relation
        self.attribute = attribute
        attribute_index = self.relation.get_attribute_index(attribute)
        self.data = []
        for t in self.relation.tuples:
            self.data.append((t[attribute_index], t))
        self.data.sort(key = self.sort_by_key)
        self.keys = [t[0] for t in self.data]

    def index(self, a, x):
        'Locate the leftmost value exactly equal to x'
        i = bisect.bisect_left(a, x)
        if i != len(a) and a[i] == x:
            return i
        return None

    def find_lt(self, a, x):
        'Find rightmost value less than x'
        i = bisect.bisect_left(a, x)
        if i:
            return i-1
        return None

    def find_le(self, a, x):
        'Find rightmost value less than or equal to x'
        i = bisect.bisect_right(a, x)
        if i:
            return i-1
        return None

    def find_gt(self, a, x):
        'Find leftmost value greater than x'
        i = bisect.bisect_right(a, x)
        if i != len(a):
            return i
        return None

    def find_ge(self, a, x):
        'Find leftmost item greater than or equal to x'
        i = bisect.bisect_left(a, x)
        if i != len(a):
            return i
        return None

    def get(self, comp_operator, key):
        res = []
        # == key
        if(comp_operator is operator.eq):
            i = self.index(self.keys, key)
            if(i is None): return res
            # get leftmost entry equal to key
            while(self.data[i][0] == key and i < len(self.data)):
                res.append(self.data[i][1])
                i = i + 1
        # > key
        elif(comp_operator is operator.gt):
            i = self.find_gt(self.keys, key)
            if(i is None): return res
            # get leftmost entry greater than key
            while(i < len(self.data) and self.data[i][0] > key):
                res.append(self.data[i][1])
                i = i + 1
        # >= key
        elif(comp_operator is operator.ge):
            i = self.find_ge(self.keys, key)
            if(i is None): return res
            # get leftmost entry greater than or equal to key
            while(i < len(self.data) and self.data[i][0] >= key):
                res.append(self.data[i][1])
                i = i + 1
        # < key
        elif(comp_operator is operator.lt):
            i = self.find_lt(self.keys, key)
            if(i is None): return res
            # get rightmost entry less than key
            while(i >= 0 and self.data[i][0] < key):
                res.append(self.data[i][1])
                i = i - 1
        # <= key
        elif(comp_operator is operator.le):
            i = self.find_le(self.keys, key)
            if(i is None): return res
            # get rightmost entry less than key
            while(i >= 0 and self.data[i][0] <= key):
                res.append(self.data[i][1])
                i = i - 1
        return res
