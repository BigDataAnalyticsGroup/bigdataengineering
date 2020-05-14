# https://docs.python.org/3.7/library/operator.html
# used here to be able to use certain built-in python operators as function parameters
import operator
from graphviz import Digraph, Source

from ra.utils import build_schema
from ra.relation import Relation
from ra.operators_log import *

# Spark Imports
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as func
from pyspark.sql import *


class LeafSpark(LeafOperator):
    """Leaf operator wrapping an input relation.

    Attributes:
        relation (:obj: `Relation`): The relation held by the leaf operator.
        dot_attrs (dic of str: str): Additional attributes for dot node (initially empty).
    """

    type_to_sparktype = {int: IntegerType(), float: FloatType(), str: StringType()}

    def __init__(self, relation, context):
        super().__init__(relation)

        # build spark dataframe from passed relation object
        self.context = context
        # create schema
        attributes = []
        for i in range(0, len(self.relation.attributes), 1):
            attributes.append(StructField(self.relation.attributes[i],
                                          self.type_to_sparktype[self.relation.domains[i]],
                                          False))
        schema = StructType(attributes)

        # build rows from relation tuples
        rows = []
        for t in self.relation.tuples:
            rows.append(Row(*list(t)))
        # create DataFrame from rows
        self.df = context.createDataFrame(rows, schema)

    def evaluate(self):
        """Evaluates the operator by returning the relation held by the leaf node."""
        return self.df


class Selection_Spark(Selection):
    def evaluate(self):
        """Performs the selection by evaluating the predicate on its input."""
        # evaluate child node
        eval_input = self.input.evaluate()
        return eval_input.filter(self.predicate)

class Projection_Spark(Projection):
    def evaluate(self):
        """Performs the projection."""
        # evaluate child node
        eval_input = self.input.evaluate()
        return eval_input.select(self.attributes)

class Cartesian_Product_Spark(Cartesian_Product):
    def evaluate(self):
        """Performs a cartesian product on its inputs."""
        # evaluate child nodes
        l_eval_input = self.l_input.evaluate()
        r_eval_input = self.r_input.evaluate()
        return l_eval_input.crossJoin(r_eval_input)


class SetOperator_Spark(SetOperator):
    def evaluate(self):
        """Performs a set operation on its inputs."""
        # evaluate child nodes
        l_eval_input = self.l_input.evaluate()
        r_eval_input = self.r_input.evaluate()

        if(self.operator is operator.and_):
            return l_eval_input.intersect(r_eval_input)
        elif(self.operator is operator.or_):
            return l_eval_input.union(r_eval_input)
        elif(self.operator is operator.sub):
            return l_eval_input.subtract(r_eval_input)
        else:
            return None

class Intersection_Spark(SetOperator_Spark):
    """The relational intersection operator

    Attributes:
        l_input (:obj: Operator): The left input to the binary operator.
        r_input (:obj: Operator): The right input to the binary operator.
    """
    def __init__(self, l_input, r_input):
        super().__init__(l_input, r_input, operator.and_, "∩")


class Union_Spark(SetOperator_Spark):
    """The relational union operator

    Attributes:
        l_input (:obj: Operator): The left input to the binary operator.
        r_input (:obj: Operator): The right input to the binary operator.
    """
    def __init__(self, l_input, r_input):
        super().__init__(l_input, r_input, operator.or_, "∪")


class Difference_Spark(SetOperator_Spark):
    """The relational difference operator

    Attributes:
        l_input (:obj: Operator): The left input to the binary operator.
        r_input (:obj: Operator): The right input to the binary operator.
    """
    def __init__(self, l_input, r_input):
        super().__init__(l_input, r_input, operator.sub, "−")


class Renaming_Relation_Spark(Renaming_Relation):
    def evaluate(self):
        return self.input.evaluate()


class Renaming_Attributes_Spark(Renaming_Attributes):
    def evaluate(self):
        # evaluate child node
        eval_input = self.input.evaluate()
        renaming = []
        for c in self.changes:
            split = c.split('<-')
            # integrity checks
            assert len(split) == 2  # after the split there should just be an old name and a new one
            assert all(map(lambda x: x.isidentifier(), split))  # the attribute names should be identifiers
            renaming.append(split[1] + " as " + split[0])
        return eval_input.selectExpr(*renaming)


class Theta_Join_Spark(Theta_Join):
    def evaluate(self):
        """Performs a theta join on its inputs."""
        # evaluate child nodes
        l_eval_input = self.l_input.evaluate()
        r_eval_input = self.r_input.evaluate()

        # build expression
        # TODO: extend for more conditions
        comp_operators = ['==', '<=', '<', '>', '>=']
        for o in comp_operators:
            if(o in self.theta):
                split = self.theta.split(o)
                assert(len(split) == 2)
                split[0] = split[0].strip()
                split[1] = split[1].strip()
                split[0] = "l_eval_input." + split[0]
                split[1] = "r_eval_input." + split[1]
                res = split[0] + str(o) + split[1]
                exp = eval(res)
                return l_eval_input.join(r_eval_input, exp)
        return None

# class Equi_Join_Spark(Equi_Join):
#     def evaluate(self):
#         # build join condition
#         # TODO: extend for more conditions
#         assert(len(self.l_attrs) == len(self.r_attrs) == 1)
#         res = self.l_attrs[0] + "==" + self.r_attrs[0]

#         theta_join = Theta_Join_Spark(self.l_input, self.r_input, res)
#         return theta_join.evaluate()

class Grouping_Spark(Grouping):
    def evaluate(self):
        """Performs grouping and aggregation on its inputs."""
        # evaluate input
        eval_input = self.input.evaluate()

        # build dictionary of aggregations
        aggregations = {}
        for a in self.aggregations:
            aggregations[a[1]] = self.builtin_to_str[a[0]]

        return eval_input.groupBy(*self.group_by).agg(aggregations)
