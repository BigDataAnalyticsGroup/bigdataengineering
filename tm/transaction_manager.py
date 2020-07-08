""" The main module that is responsible for the transaction processing. This
    module is responsible for maintaining the tables, and giving out the tran-
    saction objects to the user to perform read/write/delete operations of the
    tables.
"""

from tm.table import Table
from graphviz import Digraph
from tm.codegen import Codegen
from ra.utils import build_schema
from tm.transaction import IsolationLevel, TransactionStatus
from tm.transaction import MVCCTransaction, SVLockBasedTransaction


class TransactionManager:
    """ Class which wraps transaction processing semantics

    Attributes
    ----------
        isolation_level :IsolationLevel
            The isolation level used for transaction processing

        table_dict :dict
            The dictionary mapping the table_name to the Table object

        use_mvcc :bool
            Whether multi-version tables are used?

        global_lock_dict :dict
            The dictionary mapping (table_name, row_id) to :DummyRWLock
            object. This dictionary is used to maintain global RW locks
            for every tuple in the store.

        timestamp_counter :int
            The monotonically increasing counter used to handout timestamps
            for multi-version concurrency control. Each begin_transaction
            and commit_transaction operation increments this counter.

        transaction_list :list
            The ordered list of transactions which stores the transaction
            objects in the commit order. This list is used to detect seria-
            lization conflicts in multi-version concurrency control

        active_transactions :set
            The set containing the begin_timestamps of all active transactions.
            This list is used to clean the transaction_list. It is safe to rem-
            ove all transactions with commit timestamp less than
            min(active_transactions), because older transactions are not
            compared for serialization conflicts.
    """
    def __init__(self, isolation_level=IsolationLevel.READ_UNCOMMITTED,
                 use_mvcc=False):
        """ Instantiate the object of TransactionManager class

        Parameters
        ----------
            isolation_level :IsolationLevel
                The isolation level which is used by the TransactionManager

            use_mvcc :bool
                Should the transaction manager use multi-version store.
        """
        self.tx_id_gen = 0
        self.isolation_level = isolation_level
        self.table_dict = {}
        self.use_mvcc = use_mvcc
        self.global_lock_dict = {}

        # MVCC specific structures
        self.timestamp_counter = 0
        self.transaction_list = []
        self.active_transactions = set()

        if use_mvcc:
            assert isolation_level is IsolationLevel.SNAPSHOT_ISOLATION or \
                   isolation_level is IsolationLevel.SERIALIZABLE, \
                   "mvcc only supports SNAPSHOT_ISOLATION, and SERIALIZABLE"

        if isolation_level == IsolationLevel.SNAPSHOT_ISOLATION:
            assert use_mvcc, "SNAPSHOT_ISOLATION is only supported on MVCC \
                              store"

    def add_table(self, name, attributes=list, domains=list):
        """ Add a table to the store

        Parameters
        ----------
            name :string
                The name of the table (must be unique)

            attributes :list
                List of attribute names (must not contain row_id, begin_ts,
                and end_ts)

            domains :list
                List of attribute types (must be a valid object type in python)
        """
        assert name not in self.table_dict, "tablename already exists"
        assert "row_id" not in attributes, "attribute with name row_id is not \
                                            allowed"
        if self.use_mvcc:
            assert "begin_ts" not in attributes, "attribute with name begin_ts is not \
                                                 allowed"
            assert "end_ts" not in attributes, "attribute with name end_ts is not \
                                                 allowed"

            domains = [int, int] + domains
            attributes = ['begin_ts', 'end_ts'] + attributes

        domains = [int] + domains
        attributes = ['row_id'] + attributes

        schema = build_schema(attributes, domains)
        self.table_dict[name] = Table(name, schema, self.use_mvcc)

    def begin_transaction(self):
        """ Start a transaction

        Returns
        -------
            transaction object of type either :SVLockBasedTransaction
            or :MVCCTransaction depending upon the isolation_level
        """
        if self.use_mvcc:
            self.timestamp_counter += 1
            begin_ts = self.timestamp_counter
            self.active_transactions.add(begin_ts)
            return MVCCTransaction(self.isolation_level, self.table_dict,
                                   self.transaction_list, begin_ts)
        else:
            self.tx_id_gen += 1
            return SVLockBasedTransaction(self.tx_id_gen,
                                          self.isolation_level,
                                          self.table_dict,
                                          self.global_lock_dict)

    def _clean_transaction_list(self, min_ts):
        """ Remove old transactions from the transaction_list. This removes
            all transactions which committed before min_ts from the
            transaction_list

        Parameters
        ----------
            min_ts :int
                The minimun timestamp of an active transaction
        """
        self.transaction_list = [tx for tx in self.transaction_list
                                 if tx.commit_ts > min_ts]

    def commit_transaction(self, txn):
        """ Commit the given transaction

        Parameters
        ----------
            txn :SVLockBasedTransaction or :MVCCTransaction
                The transaction object which must be committed

        Returns
        -------
            :bool If the commit was successful or not?
        """
        assert (isinstance(txn, SVLockBasedTransaction)
                or isinstance(txn, MVCCTransaction)), "received invalid \
                transaction object"

        if self.use_mvcc:
            self.active_transactions.remove(txn.begin_ts)

            # clean up the transaction_list if there are more than 10
            # transactions in the transaction_list
            if len(self.active_transactions) > 10:
                self._clean_transaction_list(min(self.active_transactions))

            self.timestamp_counter += 1
            commit_ts = self.timestamp_counter

            if txn.commit(commit_ts):
                self.transaction_list.append(txn)
                return True
            return False
        else:
            return txn.commit()

    def abort_transaction(self, txn):
        """ Abort the given transaction

        Parameters
        ----------
            txn :SVLockBasedTransaction or :MVCCTransaction
                The transaction object which should be aborted
        """
        assert (isinstance(txn, SVLockBasedTransaction)
                or isinstance(txn, MVCCTransaction)), "received invalid \
                transaction object"

        txn.rollback()

    def execute_schedule(self, schedule, dump_exec_code=False):
        """ Execute the given schedule

        Parameters
        ----------
            schedule :str
                The schedule of transaction given as a string

            dump_exec_code :bool
                Dump the generated code to stdout?
        """

        # generate the code
        codegen = Codegen()
        variables, tx_list, pseudo_code, executable_code = \
            codegen.generate_code(schedule)

        context = self._get_context(variables, tx_list)

        # execute the generated code
        tx_status, executed_schedule = \
            self._execute(tx_list, context, executable_code)

        # remove statements of aborted transactions from executed_schedule
        for i in range(len(tx_list)):
            if i in executed_schedule and tx_status[tx_list[i]] \
                    == TransactionStatus.ABORTED:
                executed_schedule.remove(i)

        # print the status of execution
        self._print_status(tx_list, pseudo_code, executed_schedule, tx_status,
                           executable_code, dump_exec_code)

    def _print_status(self, tx_list, pseudo_code, executed_schedule, tx_status,
                      executable_code, gencode=False):
        """ Dump the result of executing the given schedule on the state """

        # print the actual schedule
        print('******************')
        print('submitted_schedule')
        print('******************')
        for i in range(len(pseudo_code)):
            print(i, "\t ", tx_list[i], "\t=> ", pseudo_code[i])

        print()
        print('*****************')
        print('executed_schedule')
        print('*****************')
        for i in executed_schedule:
            print(i, "\t ", tx_list[i], "\t=> ", pseudo_code[i])

        print()
        print('******************')
        print('transaction_status')
        print('******************')
        for key, val in tx_status.items():
            print(key, "\t => ", val)

        if gencode:
            print()
            print('***************')
            print('executable_code')
            print('***************')
            for i in executed_schedule:
                print(executable_code[i], "\n")

        print()
        print('*********************')
        print('state_after_execution')
        print('*********************')
        self.print_tables()
        print()

    def _execute(self, tx_list, context, code):
        """ Execute the generated executable code

        The executor is allowed to reorder statements across transactions bec-
        ause of locks, but not reorder statements of the same transaction.

        Parameters
        ----------
            tx_list :list
                The list of transaction's name which is scheduled at each step

            context :dict
                The dictionary containing the context for transaction execution

            code :list
                The list of generated code

            [tx1, tx2, ...]
            [c1, c2, ...]

            tx1 contains code c1, tx2 contains code c2 and so on. The list is
            arranged in the order in which the corresponding pseudocode state-
            ments appear in the submitted schedule.

        Returns
        -------
            The status of each transaction after execution of the schedule and
            the order in which the statements were actually executed. Please
            note that the statement across transactions can reorder because of
            locks.
        """
        schedule = []
        unique_tx_names = set(tx_list)

        # count the number of pending code fragments
        pending_counter = 0

        # pending queue holds [idx, code_fragment]
        pending_queue = {tx: [] for tx in unique_tx_names}

        ip = 0  # instruction pointer

        while True:
            pending_fragments = self._next_possible_fragment(pending_queue)

            if len(pending_fragments) > 0:

                if ip < len(tx_list):
                    next_tx = tx_list[ip]
                    next_code_fragment = code[ip]

                    if len(pending_queue[next_tx]) > 0:
                        pending_queue[next_tx].append([ip, next_code_fragment])
                        pending_counter += 1
                    else:
                        exec(next_code_fragment, globals(), context)
                        if not context['success']:
                            pending_queue[next_tx].append(
                                    [ip, next_code_fragment])
                            pending_counter += 1
                        else:
                            stat = context[next_tx].get_status()
                            if stat != TransactionStatus.ABORTED:
                                schedule.append(ip)
                    ip += 1

                # there are pending code fragments
                # try executing them
                for frag in pending_fragments:
                    # try executing the fragment
                    successful_frag_counter = 0
                    exec(frag[2], globals(), context)
                    if context['success']:
                        stat = context[frag[0]].get_status()
                        if stat != TransactionStatus.ABORTED:
                            schedule.append(frag[1])
                        pending_counter -= 1
                        successful_frag_counter += 1
                        # try to execute this transaction further
                        for next_frag in pending_queue[frag[0]][1:]:
                            exec(next_frag[1], globals(), context)
                            if not context['success']:
                                break
                            pending_counter -= 1
                            stat = context[frag[0]].get_status()
                            if stat != TransactionStatus.ABORTED:
                                schedule.append(next_frag[0])
                            successful_frag_counter += 1

                    pending_queue[frag[0]] = \
                        pending_queue[frag[0]][successful_frag_counter:]

            else:
                # fetch the next code fragment for queue
                if ip < len(tx_list):
                    next_tx = tx_list[ip]
                    next_code_fragment = code[ip]
                    exec(next_code_fragment, globals(), context)

                    if context['success']:
                        # execution was successful
                        stat = context[next_tx].get_status()
                        if stat != TransactionStatus.ABORTED:
                            schedule.append(ip)
                        ip += 1
                    else:
                        pending_counter += 1
                        pending_queue[tx_list[ip]].append(
                                [ip, next_code_fragment])
                        ip += 1
            if ip >= len(tx_list) and pending_counter == 0:
                break

        return {key: context[key].get_status()
                for key in unique_tx_names}, schedule

    def _get_context(self, variables, tx_names):
        """ Creates the context which maintains the state of
            transactions.

            The context contains the transaction_manager, a status flag
            which stores whether the execution was successful or not, the
            msg which may store the error which occurred which executing the
            statements, and the transaction object corresponding to every
            unique transaction in the tx_names list.

            Parameters
            ----------
                variables :set
                    The set of different variable names used by the transaction

                tx_names :set
                    The set of unique transaction names which appear in the
                    schedule.

            Returns
            -------
                The dictionary maintaining the context of the execution.
            """
        context = {var_name: None for var_name in variables}

        context['self'] = self
        context['success'] = True
        context['msg'] = ''

        for tx in tx_names:
            context[tx] = None

        return context

    def _next_possible_fragment(self, pending_queues):
        """ Get the code fragement (statements) which can be executed next.
            The list is ordered by the sequence number (the order in which they
            appear in the actual schedule)

            This function returns the oldest statements of every transaction
            which was queued for deferred execution because of some failure.

        Parameters
        ----------
            pending_queues :dict
                The dictionary mapping the transaction name to a list which
                maintains the fifo queue of statements per transaction which
                failed to execute and were deferred for later execution.
        """

        next_frags = []

        # val is [[seq_number, code_fragment], ...]
        for key, val in pending_queues.items():
            if len(val) > 0:
                next_frags.append([key, *val[0]])

        next_frags = sorted(next_frags, key=lambda x: x[1])

        return next_frags

    def print_tables(self, limit=None):
        """ Print all the tables of this transaction manager

        Parameters
        ----------
            limit :int
                The maximum number of rows per table which should be printed.
        """
        for _, table in self.table_dict.items():
            table.print_table(limit)

    def generate_precedence_graph(self, schedule):
        """ Generate the precedence graph from the schedule

        Parameters
        ----------
            schedul :list
                The list representing a valid schedule

        Returns
        -------
            The Graph object created by graphviz
        """

        codegen = Codegen()
        row_id_idx = {}

        _, tx_list, pseudo_code, _ = codegen.generate_code(schedule)

        unique_tx_names = set(tx_list)
        adjacency_list = {tx: set() for tx in unique_tx_names}

        # generate the read and write set
        for i in range(len(pseudo_code)):
            rw_set = codegen.get_read_write_set(pseudo_code[i])

            if rw_set is not None:
                if rw_set[0] not in row_id_idx:
                    row_id_idx[rw_set[0]] = []

                row_id_idx[rw_set[0]].append((tx_list[i], rw_set[1]))

        # generate the precedence graph
        for key, val in row_id_idx.items():
            for i in range(len(val)):
                for j in range(i + 1, len(val)):
                    if val[i][1] == 'r' and val[j][1] == 'w' \
                            and val[i][0] != val[j][0]:
                        # if tx[i] reads and some other tx writes to this
                        # key add an edge in the precedence graph
                        adjacency_list[val[i][0]].add(val[j][0])
                    elif val[i][0] != val[j][0]:
                        adjacency_list[val[i][0]].add(val[j][0])

        dot = Digraph(comment='Precedence Graph')

        # create nodes
        for tx in adjacency_list:
            dot.node(tx, tx)

        # create edges
        for tx, neighbours in adjacency_list.items():
            for neighbour in neighbours:
                dot.edge(tx, neighbour)

        return dot
