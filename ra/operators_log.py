import operator
import statistics

from graphviz import Digraph, Source

from ra.utils import build_schema, str_to_list
from ra.relation import Relation


################
# Base Classes #
################

class Operator:
    """Abstract base class for an operator in an operator tree."""

    def __init__(self):
        """Initializes a new Operator object."""
        pass

    def __str__(self):
        """Returns a string representation of the subtree rooted in the operator node."""
        pass

    def get_graph(self, print_source=False):
        """Computes a `graphviz.Digraph` of the operator and all its children."""
        graph = Digraph(engine='dot')
        self._dot(graph, '')
        if print_source:
            print(graph)
        graph.graph_attr['rankdir'] = 'BT'  # display graph bottom-up
        return graph

    def get_schema(self):
        """Returns the schema produced by this operator.

        Note:
            Inner nodes also call `get_schema` on their childern.

        Returns:
            A list of (attribute_name, attribute_domain) pairs representing the schema.
        """
        pass

    def _dot(self, graph, prefix):
        """Adds a node representing the operator to the graph.

        Note:
            Inner nodes also call `dot` on their children and add edges

        Args:
            graph (:obj: graphviz.Digraph): The graph a node is added to.
            prefix (:obj: `str`): The prefix for the unique identifier name, here path to the node.

        Returns:
            The name of the node added, s.t. parents can add edges from the inserted node.
        """
        pass

    def set_dot_attrs(self, attrs):
        """Sets the colors of the node for the next call of `get_graph()`.

        Args:
            attrs (dict of str: str): Additional attributes for dot node.
        """
        pass

    def close(self):
        """Cleans up resources used by this operator. Rarely useful and/or required in Python.
        """
        pass

    def has_attribute(self, attribute):
        """Tests whether attribute is part of schema
        """
        schema = self.get_schema()
        return attribute in [attr[0] for attr in schema]


class UnaryOperator(Operator):
    """Abstract base class for an internal node performing a unary operation.

    Attributes:
        input (:obj: `Operator`): The input to the unary operator
        dot_attrs (dic of str: str): Additional attributes for dot node (initially empty).
    """
    def __init__(self, input):
        self.input = input
        # dot colors
        self.dot_attrs = {}

    def _dot_helper(self, graph, prefix, name, label):
        """Helper for adding the node to the dot graph by perfoming common actions."""
        # add node to graph
        graph.node(name, label, **self.dot_attrs)
        # recursively add subtree to graph
        child_name = self.input._dot(graph, prefix+'I')
        # add edge from child to this node
        graph.edge(child_name, name)

    def set_dot_attrs(self, attrs):
        self.dot_attrs = attrs

    def close(self):
        self.input.close()


class BinaryOperator(Operator):
    """Abstract base class for an internal node performing a binary operation.

    Attributes:
        l_input (:obj: Operator): The left input to the binary operator.
        r_input (:obj: Operator): The right input to the binary operator.
        dot_attrs (dic of str: str): Additional attributes for dot node (initially empty).
    """
    def __init__(self, l_input, r_input):
        self.l_input = l_input
        self.r_input = r_input
        # dot colors
        self.dot_attrs = {}

    def _dot_helper(self, graph, prefix, name, label):
        """Helper for adding the node to the dot graph by perfoming common actions."""
        # add node to graph
        graph.node(name, label, **self.dot_attrs)
        # recursively add subtree to graph
        l_child_name = self.l_input._dot(graph, prefix+'L')
        r_child_name = self.r_input._dot(graph, prefix+'R')
        # add edge from child to this node
        graph.edge(l_child_name, name)
        graph.edge(r_child_name, name)

    def set_dot_attrs(self, attrs):
        self.dot_attrs = attrs

    def close(self):
        self.l_input.close()
        self.r_input.close()


class SetOperator(BinaryOperator):
    """Generic class for an internal node performing a set operation.

    Attributes:
        l_input (:obj: Operator): The left input to the binary operator.
        r_input (:obj: Operator): The right input to the binary operator.
        operator (function): The set operator as function from the operator module
        symbol (:obj: `str`): The symbol representig the set operation.
    """
    def __init__(self, l_input, r_input, operator, symbol):
        super().__init__(l_input, r_input)
        self.operator = operator
        self.symbol = symbol
        self.set_dot_attrs({'color':'#FF7E79', 'style': 'filled'})

    def __str__(self):
        return f'({self.l_input}) {self.symbol} ({self.r_input})'

    def _dot(self, graph, prefix, caption=""):
        # build name and label and call helper function
        node_name = prefix + 'SOp'
        node_label = self.symbol + caption
        self._dot_helper(graph, prefix, node_name, node_label)
        return node_name

    def get_schema(self):
        # evaluate child nodes
        l_eval_input = self.l_input.get_schema()
        r_eval_input = self.r_input.get_schema()

        # integrity checks (attributes and domains must be identical)
        assert l_eval_input == r_eval_input
        return l_eval_input


class LeafOperator(Operator):
    """Base class for a leaf node of an operator tree.

    Attributes:
        relation (:obj: `Relation`): The relation held by the leaf operator.
        dot_attrs (dic of str: str): Additional attributes for dot node (initially empty).
    """
    def __init__(self, relation):
        self.relation = relation
        # dot attributes
        self.dot_attrs = {}

    def __str__(self):
        return self.relation.name

    def _dot(self, graph, prefix):
        node_name = prefix + 'Rel'
        node_label = self.relation.name

        if(len(self.relation.indexes) > 0):
            node_label = node_label + '\n Index on: '
            keys = list(self.relation.indexes.keys())
            for i in range(0, len(keys), 1):
                node_label = node_label + keys[i]
                if(i < len(keys) - 1):
                    node_label = node_label + ', '

        graph.node(node_name, node_label, **self.dot_attrs)
        return node_name

    def set_dot_attrs(self, attrs):
        self.dot_attrs = attrs

    def get_schema(self):
        return build_schema(self.relation.attributes, self.relation.domains)


#####################
# Logical Operators #
#####################

class Selection(UnaryOperator):
    """The relational selection:

    Attributes:
        input (:obj: `Operator`): The input to the selection operator.
        predicate (:obj: `str`): The predicate to be evaluated by the selection on its input.
    """
    def __init__(self, input, predicate):
        super().__init__(input)
        self.predicate = predicate
        self.set_dot_attrs({'color':'#FFD479', 'style': 'filled'})

    def __str__(self):
        return f'σ_[{self.predicate}]({self.input})'

    def _dot(self, graph, prefix, caption='σ_[{}]'):
        # build name and label and call helper function
        node_name = prefix + 'Sel'
        node_label = caption.format(self.predicate)
        self._dot_helper(graph, prefix, node_name, node_label)
        return node_name

    def get_attributes_in_predicate(self):
        """
        Gets all attribute names within this predicate

        Returns:
            A set of attribute names
        """
        attribute_names = set()
        # first, split by blank (we assume expression of the form 'attribute_name op constant')
        exps = self.predicate.split(' ')
        # then, split by operator
        comp_operators = ['==', '<=', '<', '>', '>=']
        for e in exps:
            for o in comp_operators:
                splits = e.split(o)
                for s in splits:
                    if(self.has_attribute(s)):
                        attribute_names.add(s)
        return attribute_names

    def get_schema(self):
        return self.input.get_schema()

    @staticmethod
    def _locals_dict(tup, attributes):
        """
        Builds a dictionary mapping the attribute names to the corresponding value in the tuple for use in `eval`

        Args:
            tup (`tuple`): The tuple of values.
            attributes(`list` of :obj: `string`): The attribute names.

        Returns:
            A dictionary mapping attribute names to tuple values
        """
        # build dictionary
        return {attr: val for attr, val in zip(attributes, tup)}


class Projection(UnaryOperator):
    """"The relational projection:

    Attributes:
        input (:obj: `Operator`): The input to the projection operator.
        attributes (:obj: `str`): The names of the attributes the input is projected on, comma separated.
    """
    def __init__(self, input, attributes):
        super().__init__(input)
        self.attributes = str_to_list(attributes)
        self.set_dot_attrs({'color':'#76D6FF', 'style': 'filled'})

    def __str__(self):
        return f'π_[{", ".join(self.attributes)}({self.input})]'

    def _dot(self, graph, prefix, caption='π_[{}]'):
        # build name and label and call helper function
        node_name = prefix + 'Pro'
        node_label = caption.format(', '.join(self.attributes))
        self._dot_helper(graph, prefix, node_name, node_label)
        return node_name

    def get_schema(self):
        child_schema = self.input.get_schema()
        child_attrs = [attr for (attr, dom) in child_schema]
        new_schema = [child_schema[child_attrs.index(attr)] for attr in self.attributes]
        return new_schema


class Cartesian_Product(BinaryOperator):
    """The relational cartesion product

    Attributes:
        l_input (:obj: `Operator`): The left input to the cartesian product operator.
        r_input (:obj: `Operator`): The right input to the cartesian product operator.
    """
    def __init__(self, l_input, r_input):
        super().__init__(l_input, r_input)
        self.set_dot_attrs({'color':'#D4FB79', 'style': 'filled'})

    def __str__(self):
        return f'({self.l_input}) × ({self.r_input})'

    def _dot(self, graph, prefix, caption='×'):
        # build name and label and call helper function
        node_name = prefix + 'Car'
        node_label = caption
        self._dot_helper(graph, prefix, node_name, node_label)
        return node_name

    def get_schema(self):
        l_child_schema = self.l_input.get_schema()
        r_child_schema = self.r_input.get_schema()

        # integrity check (equally named attributes are not allowed here)
        l_attributes = [a for a, _ in l_child_schema]
        r_attributes = [a for a, _ in r_child_schema]
        assert len(set(l_attributes) & set(r_attributes)) == 0

        # merge schema
        return l_child_schema + r_child_schema


class Renaming_Relation(UnaryOperator):
    """"The renaming of a relation:

    Note:
        In our case renaming a relation isn't really powerful as we do not allow equal attribute names and,
        thus, dot-access (relation.attribute) is never required.

    Attributes:
        input (:obj: `Operator`): The input to the renaming operator.
        name (:obj: `str`): The new name of the result relation.
    """
    def __init__(self, input, name):
        super().__init__(input)
        self.name = name
        self.set_dot_attrs({'color':'#FF8AD8', 'style': 'filled'})

    def __str__(self):
        return f'ρ_[{self.name}]({self.input})'

    def get_schema(self):
        # get child schema
        child_schema = self.input.get_schema()
        # integrity checks
        assert self.name.isidentifier()  # the name should be an identifier
        return child_schema  # schema is left untouched

    def _dot(self, graph, prefix, caption='ρ_[{}]'):
        # build name and label and call helper function
        node_name = prefix + 'ReR'
        node_label = caption.format(self.name)
        self._dot_helper(graph, prefix, node_name, node_label)
        return node_name


class Renaming_Attributes(UnaryOperator):
    """"The renaming of attributes of a relation:

    Note:
        In our case renaming a relation isn't really powerful as we do not allow equal attribute names and,
        thus, dot-access (relation.attribute) is never required.

    Attributes:
        input (:obj: `Operator`): The input to the renaming operator.
        changes (`list` of :obj: `string`): List of changes of the form 'new_name<-old_name'.
    """
    def __init__(self, input, changes):
        super().__init__(input)
        self.changes = str_to_list(changes)
        self.set_dot_attrs({'color':'#FF8AD8', 'style': 'filled'})

    def __str__(self):
        return f'ρ_[{", ".join(self.changes)}({self.input})]'

    def get_schema(self):
        # get child schema
        child_schema = self.input.get_schema()
        # get attribute list of child schema
        new_attributes = [a for a, _ in child_schema]
        # apply each change to the attribute names
        for expr in self.changes:
            new_attributes = Renaming_Attributes._parse_attribute_rename(expr, new_attributes)
        return build_schema(new_attributes, [d for _, d in child_schema])

    def _dot(self, graph, prefix, caption='ρ_[{}]'):
        # build name and label and call helper function
        node_name = prefix + 'ReA'
        node_label = caption.format(', '.join(self.changes))
        self._dot_helper(graph, prefix, node_name, node_label)
        return node_name

    @staticmethod
    def _parse_attribute_rename(expr, attributes):
        """
        Apply the name change to the attributes

        Args:
            expr (:obj: `string`): expression describing the change of the form 'new_name<-old_name'
            attributes (`tuple` of :obj: `string`): the list of attributes that the change is to be applied to

        Returns:
            A list of attributes with the change applied.
        """
        split = expr.split('<-')
        # integrity checks
        assert len(split) == 2  # after the split there should just be an old name and a new one
        assert all(map(lambda x: x.isidentifier(), split))  # the attribute names should be identifiers
        # parse expression
        old_attr = split[1]
        new_attr = split[0]
        tmp_attributes = list(attributes)  # tuple do not allow item assignment
        for i, attr in enumerate(tmp_attributes):
            if attr == old_attr:
                tmp_attributes[i] = new_attr
                return tuple(tmp_attributes)
        raise ValueError


class Theta_Join(BinaryOperator):
    """The relational theta join

    Note:
        We implemented the theta join as a combination of cartesian product, selection, and renaming.
        This implementation has quadric runtime and should not be used in practice.

    Attributes:
        l_input (:obj: `Operator`): The left input to the difference operator.
        r_input (:obj: `Operator`): The right input to the difference operator.
        theta (:obj: `string`): The join predicate.
    """
    def __init__(self, l_input, r_input, theta):
        super().__init__(l_input, r_input)
        self.theta = theta
        self.set_dot_attrs({'color':'#FFFC79', 'style': 'filled'})

    def __str__(self):
        return f'({self.l_input}) ⋈_[{self.theta}] ({self.r_input})'

    def get_attributes_in_predicate(self):
        """
        Gets all attribute names within this predicate

        Returns:
            A set of attribute names
        """
        attribute_names = set()
        # first, split by blank (we assume expression of the form 'attribute_name op constant')
        exps = self.theta.split(' ')
        # then, split by operator
        comp_operators = ['==', '<=', '<', '>', '>=']
        for e in exps:
            for o in comp_operators:
                splits = e.split(o)
                for s in splits:
                    if(self.has_attribute(s)):
                        attribute_names.add(s)
        return attribute_names

    def get_schema(self):
        l_child_schema = self.l_input.get_schema()
        r_child_schema = self.r_input.get_schema()

        # integrity check (equally named attributes are not allowed here)
        l_attributes = [a for a, _ in l_child_schema]
        r_attributes = [a for a, _ in r_child_schema]
        assert len(set(l_attributes) & set(r_attributes)) == 0

        # merge schema
        return l_child_schema + r_child_schema

    def _dot(self, graph, prefix, caption='⋈_[{}]'):
        # build name and label and call helper function
        node_name = prefix + 'Join'
        node_label = caption.format(self.theta)
        self._dot_helper(graph, prefix, node_name, node_label)
        return node_name


class Grouping(UnaryOperator):
    """The relational grouping with aggregation.

    Note:
        We only support builtin functions for aggregation, e.g. `sum`, `max`.

    Attributes:
        input (:obj: `Operator`): The input to the renaming operator.
        group_by (:obj: `string`): The attributes the input should be grouped by, comma separated.
        aggregations (:obj: `string`): Comma separated list of aggregations.
        builtin_to_str (`dict` of builtin function to :obj: `string`):
            Dict mapping builtin function to `string` represenation.
    """
    builtin_to_str = {sum: 'sum', max:'max', min:'min', len:'count', statistics.mean:'avg'}

    def __init__(self, input, group_by, aggregations=''):
        super().__init__(input)
        self.group_by = str_to_list(group_by)
        self.aggregations = Grouping._build_aggregations(aggregations)
        self.set_dot_attrs({'color':'#7A81FF', 'style': 'filled'})

    def __str__(self):
        aggr = [f'{self.builtin_to_str[func]}({attr})' for func, attr in self.aggregations]
        return f'(γ_[{", ".join(self.group_by + aggr)}] ({self.input})'

    def get_schema(self):
        # get child schema
        child_schema = self.input.get_schema()
        # get group attributes
        grp_schema = []
        for c in child_schema:
            for g in self.group_by:
                if(g == c[0]):
                    grp_schema.append(c)
        # get aggregation attributes
        to_schema = lambda fn, attr: (self.builtin_to_str[fn]+'_'+(attr if attr != '*' else 'star'), float if fn == statistics.mean else int)
        agg_schema = [to_schema(fn, attr) for fn, attr in self.aggregations]
        # combine both schemas
        return grp_schema + agg_schema

    def _dot(self, graph, prefix, caption='γ_[{}]'):
        # build name and label and call helper function
        node_name = prefix + 'Gro'
        aggr = [f'{self.builtin_to_str[func]}({attr})' for func, attr in self.aggregations]
        node_label = caption.format(', '.join(self.group_by + aggr))
        self._dot_helper(graph, prefix, node_name, node_label)
        return node_name

    @staticmethod
    def _build_aggregations(aggregations):
        aggs = list()
        if len(aggregations) == 0:
            return
        for agg in aggregations.split(','):
            agg = agg.strip()
            fn = None
            attr = None
            if agg.startswith('count'):
                if agg == 'count(*)':
                    aggs.append((len, '*'))
                    continue
                else:
                    fn = len
                    agg = agg[6:]
            elif agg.startswith('max'):
                fn = max
                agg = agg[4:]
            elif agg.startswith('min'):
                fn = min
                agg = agg[4:]
            elif agg.startswith('sum'):
                fn = sum
                agg = agg[4:]
            elif agg.startswith('avg'):
                fn = statistics.mean
                agg = agg[4:]
            else:
                raise Exception(f'Aggregates could not be parsed, unknown aggergate function in {agg}.')
            attr = agg[:-1]
            if not attr.isalnum() or agg[-1] != ')':
                raise Exception(f'Aggregates could not be parsed, incorrect attribute format in {agg}.')
            aggs.append((fn, attr))
        return aggs


class Intersection(SetOperator):
    """The relational intersection operator

    Attributes:
        l_input (:obj: Operator): The left input to the binary operator.
        r_input (:obj: Operator): The right input to the binary operator.
    """
    def __init__(self, l_input, r_input):
        super().__init__(l_input, r_input, operator.and_, '∩')


class Union(SetOperator):
    """The relational union operator

    Attributes:
        l_input (:obj: Operator): The left input to the binary operator.
        r_input (:obj: Operator): The right input to the binary operator.
    """
    def __init__(self, l_input, r_input):
        super().__init__(l_input, r_input, operator.or_, '∪')


class Difference(SetOperator):
    """The relational difference operator

    Attributes:
        l_input (:obj: Operator): The left input to the binary operator.
        r_input (:obj: Operator): The right input to the binary operator.
    """
    def __init__(self, l_input, r_input):
        super().__init__(l_input, r_input, operator.sub, '−')

