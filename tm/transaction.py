""" Abstract Transaction class which is extended to support different
    isolation levels and concurrency control algorithms.
"""

import copy
from tm.dummy_rw_lock import DummyRWLock
from tm.enum import IsolationLevel, TransactionStatus


class Transaction:
    """ Abstract Transaction Class

    Attributes
    ----------
        table_dict :dict
            Dictionary mapping table_name to Table objects

        global_lock_dict :dict
            Dictionary mapping (table_name, row_id) to DummyRWLock object

        isolation_level :IsolationLevel
            The isolation level used by the transaction object

        status :TransactionStatus
            The status of this transaction
    """
    def __init__(self, isolation_level, table_dict, lock_dict):
        """ Instantiate an object of Transaction Class

        Parameters
        ----------
            isolation_level :IsolationLevel
                Isolation level used by this transaction object

            table_dict :dict
                Dictionary mapping the table_name to the Table object

            lock_dict :dict
                Dictionary mapping (table_name, row_id) to DummyRWLock object
        """
        assert isolation_level in [IsolationLevel.READ_UNCOMMITTED,
                                   IsolationLevel.READ_COMMITTED,
                                   IsolationLevel.REPEATABLE_READS,
                                   IsolationLevel.SERIALIZABLE,
                                   IsolationLevel.SNAPSHOT_ISOLATION]

        self.table_dict = table_dict
        self.global_lock_dict = lock_dict
        self.isolation_level = isolation_level
        self.status = TransactionStatus.RUNNING

    def get_status(self):
        """ Get the state of the current transactions

        Returns
        -------
            The :TransactionStatus of this transaction
        """
        return self.status


class SVLockBasedTransaction(Transaction):
    """ Single version, lock based transaction implementation.

    Attributes
    ----------
        read_lock_list_local :list
            The list containing the keys (table_name, rowid) of the already
            aquired read-locks

        write_lock_list_local :list
            The list containing the keys (table_name, rowid) of the already
            aquired write-locks

        local_reads :dict
            The dictionary containing the already-read tuples. Used when we
            need to guarantee repeatable reads

        last_lock_id :tuple
            The tuple (table_name, row_id) of the last acquired lock. This is
            used to avoid deadlocks. The locks are totally ordered such that
            transactions are not allowed to get a lock on a key which is
            smaller than the last_lock_id.

        original_row :dict
            The dictionary containing the original version of the tuple which
            was modified by this transaction. This is used to perform rollback
            in case the transaction gets aborted.
    """
    def __init__(self, tx_id, isolation_level, table_dict={}, lock_dict={}):
        """ Instantiate an object of SVLockBasedTransaction

        Parameters
        ----------
            isolation_level :IsolationLevel
                The isolation level used by this transaction object.

            table_dict :dict
                The dictionary containing all the tables managed by the tran-
                saction manager.

            lock_dict :dict
                The global dictionary containing the locks which are held by
                any active transactions.
        """
        super().__init__(isolation_level, table_dict, lock_dict)

        self.tx_id = tx_id

        # For tracking local reads and writes
        self.read_lock_list_local = []
        self.write_lock_list_local = []

        # dictionary of local reads
        # used for repeatable reads
        self.local_reads = {}

        # used for lock ordering
        # stored in the form (tablename, rowid)
        self.last_lock_id = ('', -1)

        # this dictionary stores the previous committed
        # state modified by the transaction. This is used
        # to rollback the transaction
        self.original_row = {}

    def _read_lock_acquire(self, dict_key):
        """ Acquire the read lock on the given key

        Parameters
        ----------
            dict_key :tuple
                (table_name, rowid) that needs to be locked

        Returns
        -------
            :tuple (msg, success)
                msg :string
                    The error message if the locking failed
                success :bool
                    True if the acquire was successful, False otherwise
        """
        msg, success = "", False
        if dict_key < self.last_lock_id:
            self.rollback()
            return ("deadlock_avoided: lock-order incorrect", False)

        if dict_key in self.read_lock_list_local:
            return ("read_lock_acquired", True)

        if dict_key in self.write_lock_list_local:
            return ("read_lock_acquired", True)

        if dict_key in self.global_lock_dict:
            msg, success = self.global_lock_dict[dict_key].try_acquire_read()
        else:
            lock = DummyRWLock()
            msg, success = lock.try_acquire_read()
            if success:
                self.global_lock_dict[dict_key] = lock

        if success:
            self.last_lock_id = dict_key
            self.read_lock_list_local.append(dict_key)

        return (msg, success)

    def _write_lock_acquire(self, dict_key):
        """ Acquire the write lock on the given key

        Parameters
        ----------
            dict_key :tuple
                (table_name, rowid) that needs to be locked

        Returns
        -------
            :tuple (msg, success)
                msg :string
                    The error message if the locking failed
                success :bool
                    True if the acquire was successful, False otherwise
        """
        if dict_key < self.last_lock_id:
            self.rollback()
            return ("deadlock_avoided: lock-order incorrect", False)

        if dict_key in self.write_lock_list_local:
            return ('write_lock_acquired', True)

        if dict_key in self.read_lock_list_local:
            # upgrade read_lock to write lock
            # first release the read-lock
            # try to acquire the write-lock
            # if write-lock acquire failed:
            # reacquire the read-lock
            self.global_lock_dict[dict_key].release_read_lock()
            msg, success = self.global_lock_dict[dict_key].try_acquire_write()
            if not success:
                # someone else is holding a read-lock
                # No one can hold a write lock, since this transaction
                # object was holding a read-lock
                lock = self.global_lock_dict[dict_key]
                lock.try_acquire_read()
                wait_for_write = lock.wait_for_write(self.tx_id)
                if wait_for_write:
                    return ('write_lock_acquire_failed', False)
                else:
                    self.rollback()
                    return ('transaction_aborted', False)
            else:
                self.read_lock_list_local.remove(dict_key)
                self.write_lock_list_local.append(dict_key)
                return ('write_lock_acquired', True)
        else:
            if dict_key not in self.global_lock_dict:
                # no one is holding any lock in this key
                lock = DummyRWLock()
                msg, success = lock.try_acquire_write()
                if success:
                    self.global_lock_dict[dict_key] = lock
                    self.write_lock_list_local.append(dict_key)
                    return (msg, success)
                else:
                    return (msg, success)
            else:
                lock = self.global_lock_dict[dict_key]
                msg, success = lock.try_acquire_write()
                if success:
                    self.write_lock_list_local.append(dict_key)
                    self.last_lock_id = dict_key
                return (msg, success)

    def read(self, table_name, rowid):
        """ Read the given row from the given table

        Parameters
        ----------
            table_name :string
                The table name

            rowid :int
                The rowid of the tuple which needs to be read

        Returns
        -------
            :tuple (msg, success, row_dict)
                msg :string
                    message describing the error if read was unsuccessful
                success :bool
                    whether the read operation was successful
                row_dict :dict
                    the dictionary mapping attribute_name to attribute _value,
                    None if the read operation failed.
        """

        assert self.status is TransactionStatus.RUNNING, \
            "attempt to read after commit"

        assert table_name in self.table_dict

        if self.isolation_level == IsolationLevel.READ_UNCOMMITTED:
            return self._read_READ_UNCOMMITED(table_name, rowid)
        elif self.isolation_level == IsolationLevel.READ_COMMITTED:
            return self._read_READ_COMMITTED(table_name, rowid)
        elif self.isolation_level == IsolationLevel.REPEATABLE_READS:
            # Considering the definition of REPEATABLE_READS used in the lectu-
            # re The only difference between REPEATABLE_READS and SERIALIZABLE
            # is that REPEATABLE_READS allows for phantom reads.
            return self._read_SERIALIZABLE(table_name, rowid)
        else:
            return self._read_SERIALIZABLE(table_name, rowid)

    def _read_READ_UNCOMMITED(self, table_name, rowid):
        """ Read the given row from the given table for read-uncommitted
            isolation level.

        Parameters
        ----------
            table_name :string
                The table name

            rowid :int
                The rowid of the tuple which needs to be read

        Returns
        -------
            :tuple (msg, success, row_dict)
                msg :string
                    message describing the error if read was unsuccessful
                success :bool
                    whether the read operation was successful
                row_dict :dict
                    the dictionary mapping attribute_name to attribute _value,
                    None if the read operation failed.
        """
        # No read locks are aquired
        table = self.table_dict[table_name]
        row = table.get(rowid)
        if len(row) > 0:
            ret_dict = table.tuple_to_dict(row[-1])
            return ('row_found', True, ret_dict)
        return ('row_not_found', False, None)

    def _read_READ_COMMITTED(self, table_name, rowid, release_lock=True):
        """ Read the given row from the given table for read-committed
            isolation level.

        Parameters
        ----------
            table_name :string
                The table name

            rowid :int
                The rowid of the tuple which needs to be read

        Returns
        -------
            :tuple (msg, success, row_dict)
                msg :string
                    message describing the error if read was unsuccessful
                success :bool
                    whether the read operation was successful
                row_dict :dict
                    the dictionary mapping attribute_name to attribute _value,
                    None if the read operation failed.
        """
        dict_key = (table_name, rowid)
        table = self.table_dict[table_name]
        msg, success = self._read_lock_acquire(dict_key)

        if not success:
            return (msg, success, None)

        row = table.get(rowid)
        if len(row) > 0:
            ret_dict = table.tuple_to_dict(row[-1])
            if release_lock:
                # release the read locks
                self.global_lock_dict[dict_key].release_read_lock()
                self.read_lock_list_local.remove(dict_key)
            return ('row_found', True, ret_dict)
        else:
            return ('row_not_found', False, None)

    def _read_REPEATABLE_READS(self, table_name, rowid, release_lock=True):
        """ Read the given row from the given table for repeatable-read
            isolation level.

        Parameters
        ----------
            table_name :string
                The table name

            rowid :int
                The rowid of the tuple which needs to be read

        Returns
        -------
            :tuple (msg, success, row_dict)
                msg :string
                    message describing the error if read was unsuccessful
                success :bool
                    whether the read operation was successful
                row_dict :dict
                    the dictionary mapping attribute_name to attribute _value,
                    None if the read operation failed.
        """
        dict_key = (table_name, rowid)
        if dict_key not in self.local_reads:
            msg, ret, ret_dict = self._read_READ_COMMITTED(table_name,
                                                           rowid, release_lock)
            if not ret:
                return (msg, ret, None)
            else:
                self.local_reads[dict_key] = ret_dict
        return ('row_found', True, self.local_reads[dict_key])

    def _read_SERIALIZABLE(self, table_name, rowid):
        """ Read the given row from the given table for serializable
            isolation level.

        Parameters
        ----------
            table_name :string
                The table name

            rowid :int
                The rowid of the tuple which needs to be read

        Returns
        -------
            :tuple (msg, success, row_dict)
                msg :string
                    message describing the error if read was unsuccessful
                success :bool
                    whether the read operation was successful
                row_dict :dict
                    the dictionary mapping attribute_name to attribute _value,
                    None if the read operation failed.
        """
        # we use 2 phase locking, so do not release read locks immediately
        # after reading
        return self._read_REPEATABLE_READS(table_name, rowid, False)

    def update(self, table_name, rowid, update_dict=dict()):
        """ Update the given row in the table

        Parameters
        ----------
            table_name :string
                The table name

            rowid :int
                The rowid of the tuple which needs to be read

            update_dict :dict
                The dictionary containing the mapping from attribute_name to
                the update value.

        Returns
        -------
            :tuple (msg, success, row_dict)
                msg :string
                    message describing the error if update was unsuccessful
                success :bool
                    whether the update operation was successful
                row_dict :dict
                    the dictionary mapping attribute_name to attribute_value
                    in a new row. None if the read operation failed.
        """
        assert self.status is TransactionStatus.RUNNING, \
            "attempt to update after commit"

        dict_key = (table_name, rowid)
        table = self.table_dict[table_name]
        msg, success = self._write_lock_acquire(dict_key)

        update_dict['row_id'] = rowid

        if not success:
            return (msg, success, None)
        else:
            row = table.get(rowid)
            if len(row) == 0:
                return ("row_not_found", False, None)
            else:
                row = copy.copy(row[-1])
                if dict_key not in self.original_row:
                    self.original_row[dict_key] = copy.copy(row)
                new_row = table.dict_to_tuple(update_dict, row)
                assert new_row is not None
                table.put(new_row)
                return ("row_updated", True, table.tuple_to_dict(new_row))

    def insert(self, table_name, insert_dict=dict()):
        """ Insert the given row in the given table.

        Parameters
        ----------
            table_name :string
                The table name

            rowid :int
                The rowid of the tuple which needs to be read

        Returns
        -------
            :tuple (msg, row_id)
                msg :string
                    message describing the error if insert was unsuccessful
                row_id :int
                    the row_id of the newly inserted row, -1 if the insert
                    failed.
        """
        assert self.status is TransactionStatus.RUNNING, \
            "attempt to insert after commit"

        table = self.table_dict[table_name]
        rowid = table.get_next_row_id()
        dict_key = (table_name, rowid)
        msg, success = self._write_lock_acquire(dict_key)

        insert_dict['row_id'] = rowid

        if not success:
            return (msg, -1)
        else:
            row = [None for i in table.attributes]
            new_row = table.dict_to_tuple(insert_dict, row)
            assert new_row is not None
            table.put(new_row)
            return ("row_inserted", rowid)

    def delete(self, table_name, rowid):
        """ Delete the given row from the given table.

        Parameters
        ----------
            table_name :string
                The table name

            rowid :int
                The rowid of the tuple which needs to be read
        """
        assert self.status is TransactionStatus.RUNNING, \
            "attempt to delete after commit"

        assert table_name in self.table_dict
        table = self.table_dict[table_name]
        dict_key = (table_name, rowid)
        msg, success = self._write_lock_acquire(dict_key)
        row = table.get(rowid)

        assert row is not None
        assert len(row) > 0

        # save the original tuple unless already saved
        if dict_key not in self.original_row:
            self.original_row[dict_key] = copy.copy(row[-1])

        # delete the tuple
        table.delete(rowid)

    def commit(self):
        """ Commit the transaction """
        # release locks and set committed to True
        if self.status == TransactionStatus.RUNNING:
            self.status = TransactionStatus.COMMITTED
            for dict_key in self.read_lock_list_local:
                if self.global_lock_dict[dict_key].release_read_lock() == 0:
                    del self.global_lock_dict[dict_key]

            for dict_key in self.write_lock_list_local:
                # there is always a single writer, so the key
                # can be removed from the global directory after
                # releasing the lock
                self.global_lock_dict[dict_key].release_write_lock()
                del self.global_lock_dict[dict_key]
            return True
        return False

    def rollback(self):
        """ Rollback the transaction """
        # rollback the updates made by the transaction
        # and release the locks

        # release locks and set committed to True
        if self.status == TransactionStatus.RUNNING:
            self.status = TransactionStatus.ABORTED
            for dict_key in self.read_lock_list_local:
                if self.global_lock_dict[dict_key].release_read_lock() == 0:
                    del self.global_lock_dict[dict_key]

            # restore the original tuples
            # locks are already held
            for dict_key in self.write_lock_list_local:
                table = self.table_dict[dict_key[0]]
                if dict_key in self.original_row:
                    table.put(self.original_row[dict_key])
                else:
                    table.delete(dict_key[1])

            for dict_key in self.write_lock_list_local:
                # there is always a single writer, so the key
                # can be removed from the global directory after
                # releasing the lock
                self.global_lock_dict[dict_key].release_write_lock()
                del self.global_lock_dict[dict_key]


class MVCCTransaction(Transaction):
    """ Multi-version optimistic concurrency control

        This implementation uses a simple  execute, validate, commit approach.

        Each transaction is assigned a begin_ts when it enters the system by
        the transaction manager. This timestamp is then used to perform read
        operations as of begin_ts. The transaction is allowed to see the state
        as of begin_ts. All updates performed after begin_ts are not visible
        to this transaction.

        Since this is multi-version store (meaning we store multiple versions
        of the row), we do not acquire any locks for the read operations.

        The updates performend by the transaction is kept in the transaction
        object (meaning they are not applied to the store until the tx commits)
        and when the transaction reads its local updates or inserts, the versi
        on is read from its local store.

        To perform a commit, the transaction applies all the updates performed
        by this transaction on the store.

        There is no additional work needed for rollback since the transaction
        did not modify any global state.

    Attributes
    ----------
        transaction_list :list
            The list of globally committed transactions which is used to verify
            serializability.

        begin_ts :int
            The begin timestamp of this transaction.

        commit_ts: int
            The commit timestamp allotted to this transaction during commit.

        read_set :set
            The set of :tuple (table_name, rowid) containing the tuples that
            was read by this transaction (for fully serializable isolation
            level).

        write_set :set
            The set of :tuple (table_name, rowid) containing the tuples that
            was updated by this transaction.

        local_updates :dict
            The dictionary containing the mapping from (table_name, rowid) to
            the dictionary containing the mapping from attribute_name to the
            attribute_value of the updated/inserted rows by this transaction.
    """
    def __init__(self, isolation_level, table_dict, transaction_list,
                 begin_ts):
        """ Instantiate an object of this class

        Parameters
        ----------
            isolation_level :IsolationLevel
                The isolation level used by this transaction which can be
                either SNAPSHOT_ISOLATION or SERIALIZABLE

            table_dict :dict
                The dictionary mapping the table_name to the Table object

            transaction_list :list
                The list containing the transaction object which are committed
                in the system. This list is used to perform validation during
                commit.

            begin_ts :int
                The begin timestamp of this transaction.
        """
        super().__init__(isolation_level, table_dict, None)

        self.transaction_list = transaction_list
        self.begin_ts = begin_ts
        self.commit_ts = 1 << 64 - 1

        self.read_set = set()
        self.write_set = set()

        self.local_updates = {}

    def read(self, table_name, rowid):
        """ Read the given row from the given table using begin_ts

        Parameters
        ----------
            table_name :string
                The table name

            rowid :int
                The row id which needs to be read

        Returns
        -------
            :tuple (msg, success, row_dict)
                msg :string
                    message describing the error if read was unsuccessful
                success :bool
                    whether the read operation was successful
                row_dict :dict
                    the dictionary mapping attribute_name to attribute _value,
                    None if the read operation failed.
        """

        assert self.status is TransactionStatus.RUNNING, \
            "attempt to read after commit"

        assert table_name in self.table_dict, "unknown table_name"
        table = self.table_dict[table_name]

        dict_key = (table_name, rowid)
        if dict_key in self.local_updates:
            # row was updated/inserted by this transaction
            row_dict = self.local_updates[dict_key]
            if row_dict['end_ts'] > self.begin_ts:
                return ('row_found', True, copy.copy(row_dict))
            else:
                # read after delete
                self.rollback()
                return ('read_after_delete, transaction_aborted', False, None)

        row = table.get(rowid)
        begin_ts_index = table.get_attribute_index('begin_ts')
        end_ts_index = table.get_attribute_index('end_ts')

        if len(row) > 0:
            for version in row:
                if version[begin_ts_index] <= self.begin_ts \
                        and version[end_ts_index] > self.begin_ts:
                    self.read_set.add(dict_key)
                    return ('row_found', True, table.tuple_to_dict(version))

        # row was not found, so abort the transaction
        self.rollback()
        return ('row_not_found, transaction_aborted', False, None)

    def update(self, table_name, rowid, update_dict):
        """ Update the given row in the table

        Parameters
        ----------
            table_name :string
                The table name

            rowid :int
                The rowid of the tuple which needs to be read

            update_dict :dict
                The dictionary containing the mapping from attribute_name to
                the update value.

        Returns
        -------
            :tuple (msg, success, row_dict)
                msg :string
                    message describing the error if update was unsuccessful
                success :bool
                    whether the update operation was successful
                row_dict :dict
                    the dictionary mapping attribute_name to attribute_value
                    in a new row. None if the read operation failed.
        """

        assert self.status is TransactionStatus.RUNNING, \
            "attempt to update after commit"

        assert table_name in self.table_dict, "unknown table_name"

        old_version = None
        dict_key = (table_name, rowid)

        if dict_key in self.local_updates:
            old_version = self.local_updates[dict_key]
        else:
            msg, success, old_version = self.read(table_name, rowid)

            # if row not found, abort the transaction and return
            if not success:
                self.rollback()
                return ("row_not_found, transaction_aborted", success, None)

        if old_version['end_ts'] > self.begin_ts:
            for key, val in update_dict.items():
                if key in old_version:
                    old_version[key] = val
                else:
                    # abort the transaction
                    self.rollback()
                    return ("wrong_attribute_name, transaction aborted",
                            False, None)
            self.local_updates[dict_key] = old_version
            self.write_set.add(dict_key)
            return ("row_updated", True, old_version)
        else:
            self.rollback()
            return ("update_after_delete, transaction_aborted", False, None)

    def insert(self, table_name, insert_dict):
        """ Insert the given row in the given table.

        Parameters
        ----------
            table_name :string
                The table name

            rowid :int
                The rowid of the tuple which needs to be read

        Returns
        -------
            :tuple (msg, row_id)
                msg :string
                    message describing the error if insert was unsuccessful
                row_id :int
                    the row_id of the newly inserted row, -1 if the insert
                    failed.
        """
        assert self.status is TransactionStatus.RUNNING, \
            "attempt to insert after commit"

        assert table_name in self.table_dict, "unknown table_name"

        table = self.table_dict[table_name]
        rowid = table.get_next_row_id()

        insert_dict['row_id'] = rowid
        insert_dict['begin_ts'] = self.begin_ts
        insert_dict['end_ts'] = 1 << 32

        dict_key = (table_name, rowid)
        self.local_updates[dict_key] = copy.copy(insert_dict)
        self.write_set.add(dict_key)

    def delete(self, table_name, rowid):
        """ Delete the given row from the given table.

        Parameters
        ----------
            table_name :string
                The table name

            rowid :int
                The rowid of the tuple which needs to be read
        """
        assert self.status is TransactionStatus.RUNNING, \
            "attempt to delete after commit"

        assert table_name in self.table_dict, "unknown table_name"

        dict_key = (table_name, rowid)
        old_version = None

        if dict_key in self.local_updates:
            old_version = self.local_updates[dict_key]
        else:
            msg, success, old_version = self.read(table_name, rowid)
            if not success:
                # row not found, abort the transaction
                self.rollback()
                return ('row_not_found, transaction_aborted', False, None)
        old_version['end_ts'] = 0
        self.local_updates[dict_key] = old_version
        self.write_set.add(dict_key)
        return ('row_deleted', True, old_version)

    def commit(self, commit_ts):
        """ Commit the transaction """
        # 1. perform read_set validation
        # 2. perforf write set_validation
        # 3. install all updates
        if self.status == TransactionStatus.RUNNING:
            self.commit_ts = commit_ts
            for tx in self.transaction_list:
                if self.begin_ts < tx.commit_ts and commit_ts > tx.commit_ts:
                    if not self.write_set.isdisjoint(tx.write_set):
                        self.rollback()
                        return False
                    if self.isolation_level != \
                            IsolationLevel.SNAPSHOT_ISOLATION:
                        if not self.read_set.isdisjoint(tx.write_set):
                            self.rollback()
                            return False

            # Install writes
            for key, val in self.local_updates.items():
                if val['end_ts'] == 0:
                    # deleted tuples
                    val['end_ts'] = commit_ts
                else:
                    # updated/inserted tuple
                    val['begin_ts'] = commit_ts

                table = self.table_dict[key[0]]
                empty_tuple = [None for i in table.attributes]
                table.put(table.dict_to_tuple(val, empty_tuple))

            self.read_set.clear()
            self.local_updates = None
            self.status = TransactionStatus.COMMITTED
            return True
        return False

    def rollback(self):
        """ Rollback the transaction """
        if self.status == TransactionStatus.RUNNING:
            self.read_set.clear()
            self.write_set.clear()
            self.local_updates = None
            self.status = TransactionStatus.ABORTED
