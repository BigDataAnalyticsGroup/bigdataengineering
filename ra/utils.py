from csv import reader
from .relation import Relation, build_schema


###############
# CSV Parsing #
###############

def load_csv(path, name, delimiter=',', quotechar='"'):
    """
    Loads a .csv File into a new relation object.

    Args:
        path (:obj: `string`): The path to the .csv file.
        name (:obj: `string`): the name of the relation to be created from the .csv file.
        delimiter (:obj: `string`): The delimiter used in the .csv file, optional.
        quotechar (:obj: `string`): The char used for quotes in the .csv file, optional.

    Returns:
        A new relation object.
    """
    # load csv into pandas df
    with open(path) as csvfile:
        csvreader = reader(csvfile, delimiter=delimiter, quotechar=quotechar)
        # extract header
        attributes = next(csvreader)
        # build attribute list and add tuples
        domains = list()
        relation = None
        for i, row in enumerate(csvreader):
            # build attribute list based on first row
            if i == 0:
                domains = get_domains(row)
                schema = build_schema(attributes, domains)
                # build relation
                relation = Relation(name, schema)
            # insert tuples
            tup = build_tuple(row, domains)
            relation.add_tuple(tup)
        return relation

def get_domains(row):
    """
    Extracts the domain types from a row in a .csv file

    Args:
        row (`list` of :obj: `string): a row from the .csv reader.

    Retruns:
        List of domain types for the provided row.
    """
    # integrity checks
    assert len(row) > 0, 'Row does not contain any data'
    # build domains list
    domains = list()
    for attr in row:
        if attr.isdigit():
            domains.append(int)
        elif isfloat(attr):
            domains.append(float)
        else:
            domains.append(str)
    return domains


def build_tuple(row, domains):
    """
    Builds a tuple to be inserted into the relation from the row and domains

    Args:
        row (`list` of :obj: `string`): `string` representation of the values a tuple is built from.
        domains (`list` of :obj: `type`): `type` of the values the tuple is built from.

    Returns:
        Tuple of accordingly typed values from the row.
    """
    # integrity checks
    assert len(row) == len(domains)  # The length of the row and domains should match
    assert all(map(lambda x: isinstance(x, type), domains))  # All domains need to be types
    # build tuple
    return tuple(dom(attr) for attr, dom in zip(row, domains))


def isfloat(input):
    """
    Determines whether a string can be interpreted as float

    Args:
        input (:obj: `string`): the string to be tested.

    Returns:
        True if the input can be interpreted as float, False otherwise.
    """
    try:
        float(input)
        return True
    except ValueError:
        return False


#################
# Miscellaneous #
#################

def str_to_list(string):
    """
    Parses a comma separated string into a list of strings.

    Args:
        string (:obj: `string`): The string, comma separated.

    Returns:
        A list of strings.
    """
    return list(map(lambda x: x.strip(), string.split(',')))


def list_to_str(lst):
    """
    Produces a comma separated string from a list of strings.

    Args:
        lst (`list` of :obj: `string`): The list of strings.

    Returns:
        A comma separated string.
    """
    return ",".join(lst)
