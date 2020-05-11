from ra.operators_log import *
from ra.operators_phys import *


##############
# Base Class #
##############

class Rule():
    """A rule that is used by the optimizer to modify the logical operator tree
    """
    def __init__(self, root):
        self.root = root

    def __str__(self):
        pass

    def _match(self, op, parent):
        """
        Matches a part of the operator tree

        Args:
            op (:obj: Operator): the operator that is currently matched
            parent (:obj: Operator): the direct parent of op

        Returns:
            True, if op matches the specified criteria
        """
        pass

    def _modify(self, op, parent):
        """
        Modifies the operator tree in the specified way, after a match has happened

        Args:
            op (:obj: Operator): the operator that has been matched
            parent (:obj: Operator): the direct parent of op

        Returns:
            cont_op (:obj: Operator): the operator to continue matching or None, if optimzation should terminate
            cont_parent (:obj: Operator): the parent of the operator to continue matching
        """
        pass

    def _replace(self, parent, top_of_old, bottom_of_old, top_of_new, bottom_of_new):
        """Replace a part of the operator tree by another operator tree

        Args:
            parent (:obj: Operator): The parent of top_of_old
            top_of_old (:obj: Operator): The top-most operator to remove
            bottom_of_old (:obj: Operator): The bottom-most operator to remove or None, if we are replacing an entire subtree
            top_of_new (:obj: Operator): The top-most operator to put
            bottom_of_new (:obj: Operator): The bottom-most operator to put or None, if we are replacing an entire subtree
        """
        # link top
        if(isinstance(parent, UnaryOperator)):
            # parent is unary
            # link top_of_new
            parent.input = top_of_new
        elif(isinstance(parent, BinaryOperator)):
            # parent is binary, so check on which side top_of_old is linked
            assert(parent.l_input == top_of_old or parent.r_input == top_of_old)
            if(parent.l_input == top_of_old):
                # left side
                parent.l_input = top_of_new
            else:
                # right side
                parent.r_input = top_of_new
        elif(parent is None):
            # we replaced the root, so update it
            self.root = top_of_new

        if(bottom_of_old is not None and bottom_of_new is not None):
            # link bottom
            if(isinstance(bottom_of_old, UnaryOperator)):
                # bottom_of_old is unary, so bottom_of_new has to be unary as well
                assert(isinstance(bottom_of_new, UnaryOperator))
                bottom_of_new.input = bottom_of_old.input
            elif(isinstance(bottom_of_old, BinaryOperator)):
                # bottom_of_old is binary, so bottom_of_new has to be binary as well
                assert(isinstance(bottom_of_new, BinaryOperator))
                bottom_of_new.l_input = bottom_of_old.l_input
                bottom_of_new.r_input = bottom_of_old.r_input

    def _delete(self, parent, op):
        """Deletes an operator from the operator tree
        Note:
            Only supported for unary operators

        Args:
            parent (:obj: Operator): The parent of op
            op (:obj: Operator): The operator to remove
        """
        assert(isinstance(op, UnaryOperator))
        child = op.input

        if(isinstance(parent, UnaryOperator)):
            # old parent is unary
            parent.input = child
        elif(isinstance(parent, BinaryOperator)):
            # old parent is binary
            assert(parent.l_input == op or parent.r_input == op)
            if(parent.l_input == op):
                parent.l_input = child
            else:
                parent.r_input = child

        if(parent is None):
            # we deleted the root, so update it
            self.root = child

    def _put(self, new_parent, op, left = True):
        """Inserts an operator into the operator tree
        Note:
            Only supported for unary operators

        Args:
            new_parent (:obj: Operator): The new parent of the operator to insert
            op (:obj: Operator): The operator to insert
            left (Boolean): If new_parent is binary, denote to which side to put the operator to insert
        """
        assert(isinstance(op, UnaryOperator))
        if(isinstance(new_parent, UnaryOperator)):
            # new_parent is unary
            grandchild = new_parent.input
            new_parent.input = op
            op.input = grandchild
        elif(isinstance(new_parent, BinaryOperator)):
            # new_parent is binary, so link it to the passed side
            if(left):
                grandchild = new_parent.l_input
                new_parent.l_input = op
                op.input = grandchild
            else:
                grandchild = new_parent.r_input
                new_parent.r_input = op
                op.input = grandchild

    def _move(self, old_parent, op, new_parent, left = True):
        """Moves an operator within the operator tree
        Note:
            Only supported for unary operators

        Args:
            old_parent (:obj: Operator): The old parent of the operator to move
            op (:obj: Operator): The operator to move
            new_parent (:obj: Operator): The new parent of the operator to move
            left (Boolean): If new_parent is binary, denote to which side to put the operator to insert
        """
        # only supported for unary operators
        assert(isinstance(op, UnaryOperator))
        child = op.input

        # 1. link old parent to child, essentially by removing op
        self._delete(old_parent, op)

        # 2. link new_parent to op and op to the respective grandchild
        self._put(new_parent, op, left)

    def optimize(self, op, parent = None):
        """
        Optimizes the tree rooted at op according to the rule

        Args:
            op (:obj: Operator): the root of the operator tree to optimize by this rule
            parent (:obj: Operator): the direct parent of op, None by default
        """
        modified = False
        # test whether op is matched by this rule
        if(self._match(op, parent)):
            # op is matched, so let's modify the operator tree according to the rule
            # cont_op: operator (possibly new) to continue the recursion
            # cont_parent: is the parent operator of cont_op in the modified tree
            cont_op, cont_parent = self._modify(op, parent)
            modified = True
            # the modification returned at which operator to continue, possibly from the root again
            if(cont_op == None):
                # Do not continue at all, so apparently, we are done
                return modified
            # continue optimization
            modified = self.optimize(cont_op, cont_parent) or modified
        else:
            # this operator does not match, so continue searching
            if(isinstance(op, UnaryOperator)):
                modified = self.optimize(op.input, op) or modified
            elif(isinstance(op, BinaryOperator)):
                modified = self.optimize(op.l_input, op) or modified
                modified = self.optimize(op.r_input, op) or modified

        return modified
