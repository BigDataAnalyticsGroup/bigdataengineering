import operator

from graphviz import Digraph, Source
from enum import Enum

from ra.utils import build_schema
from ra.relation import Relation
from ra.operators_log import *


#########
# Costs #
#########

class Costs:
    class CostModel(Enum):
        IO = 0
        Main_Memory = 1

    """An interface allowing physical operators to return cost estimates.
    """

    # statistics: dictionary with statistics about data distributions and selectivities (is available)
    def getCosts(self, size, costModelType:CostModel, statistics=None ):
        """returns the estimated costs of this physical operator.
        """
        pass

    def estimatedResultSize(self):
        """returns the estimated number of result tuples returned by this physical operator.
        """
        pass


######################
# Physical Operators #
######################

class SetOperator_HashBased(SetOperator, Costs):
    def _dot(self, graph, prefix):
        return super()._dot(graph, prefix, "_HashBased")

    def evaluate(self):
        """Performs a set operation on its inputs."""
        # evaluate child nodes
        l_eval_input = self.l_input.evaluate()
        r_eval_input = self.r_input.evaluate()

        new_relation = Relation("Result", self.get_schema())
        # add tuples to new relation
        for tup in self.operator(l_eval_input.tuples, r_eval_input.tuples):
            new_relation.add_tuple(tup)
        return new_relation

    def getCosts(size):
        pass

    def estimatedResultSize():
        pass


class LeafRelation(LeafOperator):
    def evaluate(self):
        """Evaluates the operator by returning the relation held by the leaf node."""
        return self.relation


class Selection_ScanBased(Selection):
    def __str__(self):
        return f'σ_ScanBased[{self.predicate}]({self.input})'

    def _dot(self, graph, prefix):
        return super()._dot(graph, prefix, "σ_ScanBased[{}]")

    def evaluate(self):
        """Performs the selection by evaluating the predicate on its input."""
        # evaluate child node
        eval_input = self.input.evaluate()

        # build empty relation with same schema as input
        new_relation = Relation("Result", self.get_schema())
        # check predicate for each tuple in input
        for tup in eval_input.tuples:
            # evaluate predicate for given tup using eval
            if eval(self.predicate, Selection._locals_dict(tup, eval_input.attributes)):
                new_relation.add_tuple(tup)  # implicitly handles duplicate elimination
        return new_relation


class Selection_IndexBased(Selection):
    def __str__(self):
        return f'σ_IndexBased_[{self.predicate}]({self.input})'

    def _dot(self, graph, prefix):
        return super()._dot(graph, prefix, "σ_IndexBased[{}]")

    def evaluate(self):
        """Performs the selection by evaluating the predicate on its input,
           which must be a relation containing an index on the selected predicate.

        Note:
            Currently, this access method does not use the index, but performs the access via scan.
        """
        # evaluate child node
        eval_input = self.input.evaluate()

        # build empty relation with same schema as input
        new_relation = Relation("Result", self.get_schema())
        # check predicate for each tuple in input
        for tup in eval_input.tuples:
            # evaluate predicate for given tup using eval
            if eval(self.predicate, Selection._locals_dict(tup, eval_input.attributes)):
                new_relation.add_tuple(tup)  # implicitly handles duplicate elimination
        return new_relation

    def estimatedResultSize(self):
        # TODO: replace by estimation
        return len(self.evaluate())


class Projection_ScanBased(Projection):
    def __str__(self):
        return f'π_ScanBased[{", ".join(self.attributes)}]({self.input})'

    def _dot(self, graph, prefix):
        return super()._dot(graph, prefix, 'π_ScanBased[{}]')

    def evaluate(self):
        """Performs the projection."""
        # evaluate child node
        eval_input = self.input.evaluate()

        # build new empty relation with the projected attributes
        new_relation = Relation("Result", self.get_schema())

        # add tuples to new relation
        attr_indexes = [*map(eval_input.get_attribute_index, self.attributes)]
        for tup in eval_input.tuples:
            new_tup = tuple(tup[i] for i in attr_indexes)
            new_relation.add_tuple(new_tup)  # automatically eliminates duplicates
        return new_relation


class Cartesian_Product_NestedLoop(Cartesian_Product):
    def _dot(self, graph, prefix):
        return super()._dot(graph, prefix, "×_NestedLoop")

    def evaluate(self):
        """Performs a cartesian product on its inputs."""
        # evaluate child nodes
        l_eval_input = self.l_input.evaluate()
        r_eval_input = self.r_input.evaluate()

        new_relation = Relation("Result", self.get_schema())
        # insert cartesian product of tuples
        for tup1 in l_eval_input.tuples:
            for tup2 in r_eval_input.tuples:
                new_relation.add_tuple(tup1+tup2)
        return new_relation


class Intersection_HashBased(SetOperator_HashBased):
    """The relational intersection operator

    Attributes:
        l_input (:obj: Operator): The left input to the binary operator.
        r_input (:obj: Operator): The right input to the binary operator.
    """
    def __init__(self, l_input, r_input):
        super().__init__(l_input, r_input, operator.and_, "∩")


class Union_HashBased(SetOperator_HashBased):
    """The relational union operator

    Attributes:
        l_input (:obj: Operator): The left input to the binary operator.
        r_input (:obj: Operator): The right input to the binary operator.
    """
    def __init__(self, l_input, r_input):
        super().__init__(l_input, r_input, operator.or_, "∪")


class Difference_HashBased(SetOperator_HashBased):
    """The relational difference operator

    Attributes:
        l_input (:obj: Operator): The left input to the binary operator.
        r_input (:obj: Operator): The right input to the binary operator.
    """
    def __init__(self, l_input, r_input):
        super().__init__(l_input, r_input, operator.sub, "−")


class Renaming_Relation_ScanBased(Renaming_Relation):
    def _dot(self, graph, prefix):
        return super()._dot(graph, prefix, "ρ_ScanBased[{}]")

    def evaluate(self):
        """Performs the renaming."""
        # evaluate child node
        eval_input = self.input.evaluate()
        new_relation = Relation(self.name, self.get_schema())
        # add all existing tuples
        for tup in eval_input.tuples:
            new_relation.add_tuple(tup)
        return new_relation


class Renaming_Attributes_ScanBased(Renaming_Attributes):
    def _dot(self, graph, prefix):
        return super()._dot(graph, prefix, "ρ_ScanBased[{}]")

    def evaluate(self):
        # evaluate child node
        eval_input = self.input.evaluate()
        new_relation = Relation("Result", self.get_schema())
        # add all existing tuples
        for tup in eval_input.tuples:
            new_relation.add_tuple(tup)
        return new_relation


class Theta_Join_NestedLoop(Theta_Join):
    def _dot(self, graph, prefix):
        return super()._dot(graph, prefix, "⋈_NestedLoop[{}]")

    def evaluate(self):
        """Performs a theta join on its inputs."""
        # evaluate child nodes
        l_eval_input = self.l_input.evaluate()
        r_eval_input = self.r_input.evaluate()

        new_relation = Relation("Result", self.get_schema())
        # insert cartesian product of tuples
        for tup1 in l_eval_input.tuples:
            for tup2 in r_eval_input.tuples:
                potential_tup = tup1+tup2
                if eval(self.theta, Selection._locals_dict(potential_tup, new_relation.attributes)):
                    new_relation.add_tuple(potential_tup)  # implicitly handles duplicate elimination
        return new_relation


class Grouping_HashBased(Grouping):
    def _dot(self, graph, prefix):
        return super()._dot(graph, prefix, "γ_HashBased[{}]")

    def evaluate(self):
        """Performs grouping and aggregation on its inputs."""
        # evaluate input
        eval_input = self.input.evaluate()
        # build groups
        groups = self._build_groups(eval_input)
        # compute aggregations
        tuples = self._compute_aggregations(eval_input, groups)
        # build new relation
        new_relation = Relation("Result", self.get_schema())
        # insert tuples into relation
        for tup in tuples:
            new_relation.add_tuple(tup)
        return new_relation

    def _build_groups(self, eval_input):
        """Builds the groups from the evaluated input."""
        groups = dict()  # maps group to tuples in group
        idxs =  [eval_input.get_attribute_index(attr) for attr in self.group_by] # get indexes of attributes in group
        # insert each tuple in corresponding group
        for tup in eval_input.tuples:
            key = tuple(tup[i] for i in idxs)  # determine group of tuple
            group = groups.get(key, [])  # retrieve group from map, default: empty list
            groups[key] = group+[tup]  # add tuple to group
        return groups

    def _compute_aggregations(self, eval_input, groups):
        """Computes aggregations on partitioned groups."""
        results = set()  # result set of tuples, containing group attributes and aggregates
        # for each group, compute aggregates
        for key in groups:
            result_tup = key  # first part of result tuple is the group
            group = groups[key]  # get list of group members to compute aggregations on
            # compute each aggregate
            for fn, attr in self.aggregations:
                if fn == len and attr == "*":
                    agg_result = len(group)  # apply aggregation function to list of attributes
                    result_tup += (agg_result, )  # append aggregation result to result tuple
                else:
                    idx = eval_input.get_attribute_index(attr)  # position of attribute within each tuple
                    agg_result = fn([x[idx] for x in group])  # apply aggregation function to list of attributes
                    if fn == statistics.mean:
                        agg_result = float(agg_result)
                    result_tup += (agg_result, )  # append aggregation result to result tuple
            results.add(result_tup)
        return results
