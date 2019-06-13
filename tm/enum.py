""" This file defines the Enums used by different components of the
    tm module.
"""

from enum import Enum


class IsolationLevel(Enum):
    """ The Enum defining different isolation levels of a transactional
        system.

        SERIALIZABLE: No anomalies allowed in the serialization schedule.

        READ_COMMITTED: The transaction is allowed to see the most recent
                        committed state as of the read operation. The transa-
                        ction might see different value of the same tuple if
                        it reads the same tuple at two different point in time

        READ_UNCOMMITTED: The transaction is allowed to see the most recent
                          update done by any transaction (even uncommitted).
                        The transaction might see different value of the same
                        tuple if it reads the same tuple at two different point
                        in time

        REPEATABLE_READS: The transaction can see only committed value. The
                          The transaction can repeatedly see the same tuple
                          version throughout its life, even if the tuple was
                          modified by another committed transaction.

        SNAPSHOT_ISOLATION: The tranaction sees the snapshot of the state as
                            of the time when it started. None of the changes
                            which were made by any committed transaction is
                            visible to this transaction.
    """
    SERIALIZABLE = 1
    READ_COMMITTED = 2
    READ_UNCOMMITTED = 3
    REPEATABLE_READS = 4
    SNAPSHOT_ISOLATION = 5


class TransactionStatus(Enum):
    """ The state of the transaction

        RUNNING: The transaction is still running

        ABORTED: The transaction was aborted either by the user, because of
                 serialization conflict, or because of deadlock avoidance
                 algorithm.

        COMMITTED: The transaction was committed successfully.
    """
    RUNNING = 1
    ABORTED = 2
    COMMITTED = 3
