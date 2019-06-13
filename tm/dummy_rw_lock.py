""" A Simple Lock Mocking Library

    Single Writer/Multiple Readers

    Note: this library mocks standard mutex for concurrent access.
          Please note that this library is not designed for parallel
          execution

    This library DOES NOT implement a traditional SharedLock (RWLock).
    DO NOT USE this library if your application does parallel execution.

    This library does not validate the lock owners. So the lock can be
    released by someone who is not an owner of the lock.
"""


class DummyRWLock:
    """DummyRWLock which mocks shared lock for concurrent but NOT
       parallel access.

    Attributes
    ----------
        readers: int
            The number of active readers
        active_writer: bool
            Whether there is an active writer?
        wait: bool
            Should the next write-lock requester wait? If yes, it
            will retry the acquire later on. This is done to make
            sure that the first requester of the write lock can get
            the write lock.
    """

    def __init__(self):
        """ Instantiate an object of DummyRWLock class"""
        self.readers = 0
        self.active_writer = False
        self.requestor = None

    def try_acquire_read(self):
        """ Non-blocking method which can be used to acquire a read-lock.

        Returns
        -------
            :tuple (msg :str, success :bool)
                msg: the message describing the error if any (for verbose)
                success: whether the acquire operation was successful?
        """
        if not self.active_writer:
            self.readers += 1
            return ('read_lock_acquired', True)
        return ('write_lock_already_held', False)

    def try_acquire_write(self):
        """ Non-blocking method which can be used to acquire a write-lock.

        Returns
        -------
            :tuple (msg :str, success :bool)
                msg: the message describing the error if any (for verbose)
                success: whether the acquire operation was successful?
        """
        if self.active_writer:
            return ('write_lock_already_held', False)

        if self.readers != 0:
            return ('read_lock_already_held', False)

        self.active_writer = True
        return ('write_lock_acquired', True)

    def release_read_lock(self):
        """ Non-blocking method which can be used to release a read-lock."""
        if self.readers > 0:
            self.readers -= 1

    def release_write_lock(self):
        """ Non-blocking method which can be used to release a write-lock."""
        if self.active_writer:
            self.active_writer = False
            # Allow other writers to wait for write lock
            self.requestor = None

    def wait_for_write(self, requestor):
        """ Check whether the writer is allowed to wait for the write lock.
            This may happen if there are active readers. The writer can retry
            when all read locks are released.

        Returns
        -------
            True: if no other writer is waiting for the write lock
            False: otherwise
        """
        if self.requestor is None or self.requestor == requestor:
            self.requestor = requestor
            return True
        return False

    def __str__(self):
        """ Get the string representation of the DummyRWLock object"""
        return "readers:{}, writers:{}".format(self.readers,
                                               self.active_writer)
