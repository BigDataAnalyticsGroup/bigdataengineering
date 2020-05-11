from ra.rule import *


class BreakUpSelections(Rule):
    """"BreakUpSelections Class

    Note:
        Breaks up simple compound selections, where the predicates are concatenated via 'and'.
    """
    def __init__(self, root):
        super().__init__(root)

    def _is_compound_selection(self, op):
        # ... check whether it is a compound selection
        # Note: for simplicity, we assume predicates of the form 'A op x and B op y and C op z and ...'
        # and reject anything else

        # if there are any brackets, reject directly
        brackets = r'()'
        if(any(elem in op.predicate for elem in brackets)):
            return False

        # first split by ' and '
        subpredicates = op.predicate.split(' and ')
        if(len(subpredicates) < 2):
            return False

        comp_operators = ['==', '<=', '<', '>', '>=']
        for s in subpredicates:
            # next, split by comparison operator
            for o in comp_operators:
                if(o in s):
                    splits = s.split(o)
                    if(len(splits) != 2):
                        # we should get 2 expression, but we don't, so reject selection
                        return False
        return True

    def _match(self, op, parent):
        # For each selection operator found ...
        if(isinstance(op, Selection)):
            # ... check whether it is a compound selection
            return self._is_compound_selection(op)

    def _modify(self, op, parent):
        # get chain of selections (bottom up)
        selections = self._split_compound_selection(op)

        # replace compound selection by new selection chain
        self._replace(parent, op, op, selections[len(selections)-1], selections[0])

        # continue optimization on the operator below the bottom most selection in the chain
        return selections[0].input, selections[0]

    def _split_compound_selection(self, op):
        """Splits an 'and' compound selection

        Args:
            op (:obj: Operator): The compound selection to split

        Returns:
            The chain of Selection objects, represting the split selection
        """
        # split the predicate
        predicates = op.predicate.split(' and ')
        selections = []
        # build the selection chain bottom up
        sel = op.input
        for p in predicates:
            sel = Selection(sel, p)
            selections.append(sel)

        return selections


class PushDownSelection(Rule):
    """"PushDownSelection Class

    Note:
        Note that this rule pushes a selection only to its grandchild.
        A full push-down is realized by repetitive application of this rule.

    Attributes:
        root (:obj: `Operator`): The root of the tree to optimize
    """
    def __init__(self, root):
        super().__init__(root)
        # keep track of all selections that have been fully pushed and should not be considered anymore
        self.pushed_selections = set()

    def _match(self, op, parent):
        # find a selection, which has not been fully pushed down yet
        return (isinstance(op, Selection) and (id(op) not in self.pushed_selections))

    def _modify(self, op, parent):
        """Tries to push down the selection above its grandchild(s)"""
        assert(isinstance(op, Selection))
        attribute_names = op.get_attributes_in_predicate()

        # op is a selection, so it has exactly one child
        child = op.input

        # is this child unary or binary?
        if(isinstance(child, UnaryOperator)):
            # it is unary, so inspect its single grandchild
            grandchild = child.input
            # does the grandchild contain all attributes of our selection predicate?
            if(self._is_push_down_possible(attribute_names, grandchild)):
                # push-down selection below child
                self._move(parent, op, child)
                # continue the next round at the new position of the selection
                return op, child
            else:
                # it does not contain all attributes, so we can not push down this selection any further
                # add it to the list of worked selections ...
                return self._selection_fully_pushed(op)
        elif(isinstance(child, BinaryOperator)):
            # it is binary, so inspect its both grandchildren
            push_left = self._is_push_down_possible(attribute_names, child.l_input)
            push_right = self._is_push_down_possible(attribute_names, child.r_input)

            if(push_left and push_right):
                # special case: we have to duplicate the selection
                sel1 = Selection(op.input, op.predicate)
                sel2 = Selection(op.input, op.predicate)
                # delete old selection
                self._delete(parent, op)
                # put new selection
                self._put(child, sel1, True)
                self._put(child, sel2, False)
                return child, parent
            else:
                if(push_left):
                    # push above left grandchild
                    self._move(parent, op, child, True)
                    return op, child
                if(push_right):
                    # push above right grandchild
                    self._move(parent, op, child, False)
                    return op, child
            # if we reach this point, we haven't pushed the selection to any side
            # thus, it is fully pushed down
            return self._selection_fully_pushed(op)
        else:
            # the selection sits above a leaf relation, so it is fully pushed down
            return self._selection_fully_pushed(op)

    def _is_push_down_possible(self, attribute_names, grandchild):
        """
        Tests whether grandchild contains all attributes

        Note:
            For simplicity, we assume only equality predicates.
            Further, we assume that optimization 1 has been applied already.

        Args:
            attribute_names(`set` of :obj: `string`): The attribute names to test
            grandchild(:obj: `Operator`): The grandchild to test

        Returns:
            True, if grandchild contains all attributes
        """
        for a in attribute_names:
            if not(grandchild.has_attribute(a)):
                return False
        return True

    def _selection_fully_pushed(self, op):
        """
        Adds a selection to the list of fully pushed selections

        Args:
            op(:obj: `Operator`): The operator to add to the list of fully pushed down selections

        Returns:
            The root and its parent (None) to restart optimizatin
        """
        assert(isinstance(op, Selection))
        # add to the list
        self.pushed_selections.add(id(op))
        # restart optimization at the root
        return self.root, None


class ReplaceByJoin(Rule):
    """"ReplaceByJoin Class

    Note:
        This assumes that optimization 1 and optimization 2 have been already applied, i.e.
        the join-style selection is the direct parent of the cartesian product
        Further, for simplicity it assumes only equi-join selections
    """
    def __init__(self, root):
        super().__init__(root)

    def _match(self, op, parent):
        # 2. For each selection found ...
        if(isinstance(op, Selection)):
            # ... check whether it has a cartesian product on both input relations as child
            # (note that this assumes that a selection pushdown happened already)
            if(isinstance(op.input, Cartesian_Product)):
                cp = op.input
                # the child is a cartesian product, but does this selection contain its join predicate?
                # split by comparison operator
                attributes = []
                comp_operators = ['==', '<=', '<', '>', '>=']
                for o in comp_operators:
                    if(o in op.predicate):
                        attributes = op.predicate.split(o)
                        break
                if (len(attributes) == 2):
                    # the selection might contain a join predicates
                    # test whether each attribute belongs to one of the relations of the cartesian product
                    return((cp.l_input.has_attribute(attributes[0].strip())
                            and cp.r_input.has_attribute(attributes[1].strip())) or
                           (cp.l_input.has_attribute(attributes[1].strip())
                            and cp.r_input.has_attribute(attributes[0].strip())))

    def _modify(self, op, parent):
        cp = op.input

        # so let's replace them with a join
        join = Theta_Join(cp.l_input, cp.r_input, op.predicate)

        # replace selection and cp with join
        self._replace(parent, op, cp, join, join)

        # continue traversal at the new join node
        return join, parent


class InsertProjection(Rule):
    """"InsertProjection Class
    """
    def __init__(self, root):
        super().__init__(root)
        self.processed_operators = set()

        # annotate entire tree
        self._annotate(root, None)

    def _match(self, op, parent):
        # match if op has not been processed so far and it is not the root
        if(op not in self.processed_operators and parent is not None):
            # check if the parent is not already a projection
            if not(isinstance(parent, Projection)):
                # check if this is the right position for the projection
                # if the operator below provides the same attributes, then do not add the projection here
                if(isinstance(op, UnaryOperator)):
                    if(self._get_provided_attributes(op, parent) == self._get_provided_attributes(op.input, op)):
                        return False
                elif(isinstance(op, BinaryOperator)):
                    if(self._get_provided_attributes(op, parent) == self._get_provided_attributes(op.l_input, op)
                       == self._get_provided_attributes(op.r_input, op)):
                        return False
                return True
        return False

    def _modify(self, op, parent):
        # add this operator to the set of processed operators, such that it is not matched again
        self.processed_operators.add(op)

        # compute from the required attributes of the parent what to project here
        provided_attributes = self._get_provided_attributes(op, parent)

        # create the projection
        proj = Projection(op, ','.join(provided_attributes))
        proj.required_attributes = provided_attributes

        # put the projection
        if(isinstance(parent, BinaryOperator)):
            assert(parent.l_input == op or parent.r_input == op)
            self._put(parent, proj, parent.l_input == op)
        else:
            self._put(parent, proj)

        return op, proj

    def _get_provided_attributes(self, op, parent):
        provided_attributes = set()
        for p in parent.required_attributes:
            if(op.has_attribute(p)):
                provided_attributes.add(p)
        return provided_attributes

    def _annotate(self, op, parent):
        self._annotate_node(op, parent)

        if(isinstance(op, UnaryOperator)):
            self._annotate(op.input, op)
        elif(isinstance(op, BinaryOperator)):
            self._annotate(op.l_input, op)
            self._annotate(op.r_input, op)

    def _annotate_node(self, op, parent):
        """
        Annotes the operator with the required operators

        Args:
            op(:obj: `Operator`): The operator to annotate
            parent(:obj: `Operator`): The parent of op
        """
        # compute the attributes required by op
        required_attributes = self._get_required_attributes(op)
        if (parent is None):
            # there is no parent, so these attributes are all we require
            op.required_attributes = required_attributes
        else:
            # there is a parent, so take the attributes it requires and compute which one op contains as well
            relevant_parent_attributes = set()
            for p in parent.required_attributes:
                if(op.has_attribute(p)):
                    relevant_parent_attributes.add(p)
            op.required_attributes = relevant_parent_attributes | required_attributes

    def _get_required_attributes(self, op):
        """
        Get all attributes, that are part of the relation, which results from evaluating op

        Args:
            op(:obj: `Operator`): The operator to inspect

        Returns:
            The set of contained attributes
        """
        attributes = set()

        if(isinstance(op, Selection)):
            # a selection requires the attributes contained in its predicate
            # Assumes that there are blanks between attribute name and comparison operator
            attributes = op.get_attributes_in_predicate()
        elif(isinstance(op, Projection)):
            # a projection requires the attributes it projects on
            attributes = set(op.attributes)
        elif(isinstance(op, Theta_Join)):
            # a theta join requires the attributes contained in its join predicate
            # Assumes that there are blanks between attribute name and comparison operator
            attributes = op.get_attributes_in_predicate()

        # all other operators do not have requirements

        return attributes

