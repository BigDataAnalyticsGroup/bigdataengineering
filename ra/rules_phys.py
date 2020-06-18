from ra.rule import *
from ra.utils import *


class CompileSetOperator(Rule):
    """"CompileSetOperator Class

    Note:
        Compiles a logical operator into a physical operator
    """
    def _match(self, op, parent):
        return isinstance(op, SetOperator) and not isinstance(op, SetOperator_HashBased)

    def _modify(self, op, parent):
        physical_op = SetOperator_HashBased(op.l_input, op.r_input, op.operator, op.symbol)
        self._replace(parent, op, op, physical_op, physical_op)
        return physical_op, parent


class CompileSelectionScan(Rule):
    """"CompileSelectionScan Class

    Note:
        Compiles a logical operator into a physical operator
    """
    def _match(self, op, parent):
        match = isinstance(op, Selection) \
        and not isinstance(op, Selection_ScanBased) \
        and not isinstance(op, Selection_IndexBased)
        return match

    def _modify(self, op, parent):
        physical_op = Selection_ScanBased(op.input, op.predicate)
        self._replace(parent, op, op, physical_op, physical_op)
        return physical_op.input, physical_op


class CompileSelectionIndex(Rule):
    """"CompileSelection Class

    Note:
        Compiles a logical operator into a physical operator
    """

    def _match(self, op, parent):
        # match only a selection, if ...
        # 1. there are only projections, selections, and a leaf relation below it
        # 2. there is an index in the leaf relation on an attribute used it its predicate

        # find a logical selection
        if(isinstance(op, Selection) and
           not isinstance(op, Selection_ScanBased) and
           not isinstance(op, Selection_IndexBased)):
            # if the selections accesses more than one attribute, directly reject it
            if(len(op.get_attributes_in_predicate()) > 1):
                return False

            # found one, so traverse down the tree to the leaf relation
            child = op.input
            while(not isinstance(child, LeafOperator)):
                if(not isinstance(child, Projection) and not isinstance(child, Selection)):
                    # encountered something different, so do not match
                    return False
                child = child.input

            assert(isinstance(child, LeafOperator))
            leaf = child

            # we reached the leaf relation. check whether it contains a suitable index for our selection
            attributes = op.get_attributes_in_predicate()
            for a in attributes:
                if(leaf.relation.has_index_on(a)):
                    return True
            return False

    def _modify(self, op, parent):
        selections = []
        selections.append(op)
        # 1. We know, that op is a selection for which an index in the leaf exists
        #    Let us now check whether there are other selections on the way, which could also use an index
        child = op.input
        while(not isinstance(child, LeafOperator)):
            if(isinstance(child, Selection)):
                # we found another selection
                selections.append(child)
            child = child.input
        leaf = child

        # 2. Compute for each selection, which index it could use
        selections_with_index = []
        for sel in selections:
            attributes = sel.get_attributes_in_predicate()
            for a in attributes:
                if(leaf.relation.has_index_on(a)):
                    index = leaf.relation.get_index_on(a)
                    selections_with_index.append(sel)
                    break

        assert(len(selections_with_index) > 0 and len(selections_with_index) <= len(selections))

        # 3. estimate which selection has the highest selectivity
        # hard-coded for now: simply assume first one is the one with the highest selectivity
        min_result_size = len(leaf.relation)
        picked_sel = None
        for sel in selections_with_index:
            sel_to_test = Selection_IndexBased(leaf, sel.predicate)
            size = sel_to_test.estimatedResultSize()
            if(size < min_result_size):
                min_result_size = size
                picked_sel = sel

        picked_sel_index = selections.index(picked_sel)

        # 4. perform the index based selection with the one with the highest selecitivty
        # 5. add the remaining selections on top
        next_sel = Selection_IndexBased(leaf, selections[picked_sel_index].predicate)
        for i in range(0, len(selections), 1):
            if(i != picked_sel_index):
                next_sel = Selection_ScanBased(next_sel, selections[i].predicate)

        self._replace(parent, op, leaf, next_sel, leaf)

        return self.root, None


class CompileProjection(Rule):
    """"CompileProjection Class

    Note:
        Compiles a logical operator into a physical operator
    """
    def _match(self, op, parent):
        return isinstance(op, Projection) and not isinstance(op, Projection_ScanBased)

    def _modify(self, op, parent):
        physical_op = Projection_ScanBased(op.input, list_to_str(op.attributes))
        self._replace(parent, op, op, physical_op, physical_op)
        return physical_op.input, physical_op


class CompileCartesianProduct(Rule):
    """"CompileCartesianProduct Class

    Note:
        Compiles a logical operator into a physical operator
    """
    def _match(self, op, parent):
        return isinstance(op, Cartesian_Product) and not isinstance(op, Cartesian_Product_NestedLoop)

    def _modify(self, op, parent):
        physical_op = Cartesian_Product_NestedLoop(op.l_input, op.r_input)
        self._replace(parent, op, op, physical_op, physical_op)
        return physical_op, parent


class CompileRenamingRelation(Rule):
    """"CompileRenamingRelation Class

    Note:
        Compiles a logical operator into a physical operator
    """
    def _match(self, op, parent):
        return isinstance(op, Renaming_Relation) and not isinstance(op, Renaming_Relation_ScanBased)

    def _modify(self, op, parent):
        physical_op = Renaming_Relation_ScanBased(op.input, op.name)
        self._replace(parent, op, op, physical_op, physical_op)
        return physical_op, parent


class CompileRenamingAttributes(Rule):
    """"CompileRenamingAttributes Class

    Note:
        Compiles a logical operator into a physical operator
    """
    def _match(self, op, parent):
        return isinstance(op, Renaming_Attributes) and not isinstance(op, Renaming_Attributes_ScanBased)

    def _modify(self, op, parent):
        physical_op = Renaming_Attributes_ScanBased(op.input, op.changes)
        self._replace(parent, op, op, physical_op, physical_op)
        return physical_op, parent


class CompileThetaJoin(Rule):
    """"CompileThetaJoin Class

    Note:
        Compiles a logical operator into a physical operator
    """
    def _match(self, op, parent):
        return isinstance(op, Theta_Join) and not isinstance(op, Theta_Join_NestedLoop)

    def _modify(self, op, parent):
        physical_op = Theta_Join_NestedLoop(op.l_input, op.r_input, op.theta)
        self._replace(parent, op, op, physical_op, physical_op)
        return physical_op, parent


class CompileGrouping(Rule):
    """"CompileGrouping Class

    Note:
        Compiles a logical operator into a physical operator
    """
    def _match(self, op, parent):
        return isinstance(op, Grouping) and not isinstance(op, Grouping_HashBased)

    def _modify(self, op, parent):
        physical_op = Grouping_HashBased(op.input, list_to_str(op.group_by), "")
        physical_op.aggregations = op.aggregations  # overwrite aggregations, conversion via string
        self._replace(parent, op, op, physical_op, physical_op)
        return physical_op, parent


def compile_plan(root):
    # compile logical to physical operators
    operators_to_compile = [CompileSetOperator,
                            CompileSelectionIndex,
                            CompileSelectionScan,
                            CompileProjection,
                            CompileCartesianProduct,
                            CompileRenamingRelation,
                            CompileRenamingAttributes,
                            CompileThetaJoin,
                            CompileGrouping]

    last_root = root
    for OperatorToCompile in operators_to_compile:
        rule = OperatorToCompile(last_root)
        rule.optimize(last_root)
        last_root = rule.root
    return last_root

