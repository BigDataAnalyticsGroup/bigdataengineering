####################
# Relation Helpers #
####################

def build_schema(attributes, domains):
    """
    Builds the schema needed to initialize a new relation object
    
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
    """
    Builds attributes and domains tuples from schema

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
