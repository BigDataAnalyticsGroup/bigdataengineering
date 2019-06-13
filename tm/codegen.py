""" This module generates python executable code for a minimal
    pseudocode language supporting following statements

    1. val = READ(table_name=, rowid=, column=)
        Read the given column of given row from the table

    2. UPDATE(table_name=, rowid=, values={'col1': 10})
        Update the given values of the given row in the table

    3. INSERT(table_name=, values={'col1': 10})
        Insert the given row in the table

    4. DELETE(table_name=, rowid=)
        Delete the given row from the table

    5. ASSERT(constraint=)
        Assert the given constraint. The transaction aborts if
        the constraint is violated.

    6. COMMIT()
        Commit the current transaction

    7. ABORT()
        Abort the current transaction
"""

import re


class Codegen:
    """ Generate code from given pseudocode

    Attributes
    ----------
        read_re :str
            regular expression user to parse READ statements
            e.g. val = READ(table_name=sample_table,
                            rowid=0, column=Balance)

            strings must be written without quotes.

        update_re :str
            regular expression user to parse UPDATE statements
            e.g. UPDATE(table_name=sample_table,
                        rowid=0, values={'Balance': 10.0})

            strings must be written without quotes, except for
            values dictionary.

        insert_re :str
            regular expression user to parse INSERT statements
            e.g. INSERT(table_name=sample_table,
                        values={'Balance': 10.0})

            strings must be written without quotes, except for
            values dictionary.

        delete_re :str
            regular expression user to parse DELETE statements
            e.g. DELETE(table_name=sample_table, rowid=0)

            strings must be written without quotes

        read_template :str
            template used to generate python code corresponding to READ
            statement. The parameters are parsed from the pseudocode using
            read_re and inserted into this template to perform read operation.

        update_template :str
            template used to generate python code corresponding to UPDATE
            statement. The parameters are parsed from the pseudocode using
            update_re and inserted into this template to perform update
            operation.

        insert_template :str
            template used to generate python code corresponding to INSERT
            statement. The parameters are parsed from the pseudocode using
            insert_re and inserted into this template to perform insert
            operation.

        delete_template :str
            template used to generate python code corresponding to DELETE
            statement. The parameters are parsed from the pseudocode using
            delete_re and inserted into this template to perform delete
            operation.

        assert_template :str
            template used to generate python code corresponding to ASSERT
            statement. The parameters are parsed from the pseudocode using
            assert_re and inserted into this template to perform assert
            operation.

        commit_template :str
            template used to generate python code corresponding to COMMIT
            statement. The parameters are parsed from the pseudocode and
            inserted into this template to perform commit operation.

        abort_template :str
            template used to generate python code corresponding to ABORT
            statement. The parameters are parsed from the pseudocode and
            inserted into this template to perform abort operation.
    """

    def __init__(self):
        """ Initialize an object of Codegen class """
        self.read_re = (
                r"([^ ]*)\s*=\s*READ\s*\(\s*table_name\s*="
                r"\s*([^ ]*)\s*,\s*rowid\s*=\s*([^ ]*)\s*,"
                r"\s*column\s*=\s*(.*)\)"
            )

        self.update_re = (
                r"\s*UPDATE\s*\(\s*table_name\s*=\s*([^ ]*)"
                r"\s*,\s*rowid\s*=\s*([^ ]*)\s*,\s*values\s*=\s*(.*)\)"
            )

        self.insert_re = (
                r"\s*INSERT\s*\(\s*table_name\s*=\s*([^ ]*)\s*,\s*"
                r"values\s*=\s*(.*)\)"
            )

        self.delete_re = (
                r"\s*DELETE\s*\(\s*table_name\s*=\s*([^ ]*)\s*,\s*"
                r"rowid\s*=\s*([^ ]*)\)"
            )

        self.assert_re = r"\s*ASSERT\s*\(\s*constraint\s*=\s*(.*)\)"

        self.read_template = (
            "assert {tx} is not None, '{tx} is not initialized, use BEGIN()'\n"
            "success = True\n"
            "if {tx}.get_status() == TransactionStatus.RUNNING:\n"
            "   {var_name} = None\n"
            "   msg, success, row = {tx}.read(table_name=\"{table_name}\","
            "rowid={row_id})\n"
            "   if success:\n"
            "       assert \"{column}\" in row, \"column: {column} not "
            "found in\" + str(row)\n"
            "       {var_name} = row[\"{column}\"]"
        )

        self.update_template = (
            "assert {tx} is not None, '{tx} is not initialized, use BEGIN()'\n"
            "success = True\n"
            "if {tx}.get_status() == TransactionStatus.RUNNING:\n"
            "   msg, success, _ = {tx}.update("
            "   table_name=\"{table_name}\", "
            "   rowid={row_id}, update_dict={update})"
        )

        self.insert_template = (
            "assert {tx} is not None, '{tx} is not initialized, use BEGIN()'\n"
            "success = True\n"
            "if {tx}.get_status() == TransactionStatus.RUNNING:\n"
            "   msg, row_id = {tx}.insert(table_name=\"{table_name}\", "
            "   insert_dict={insert})\n"
            "   assert row_id > -1, 'insertion failed'\n"
        )

        self.delete_template = (
            "assert {tx} is not None, '{tx} is not initialized, use BEGIN()'\n"
            "success = True\n"
            "if {tx}.get_status() == TransactionStatus.RUNNING:\n"
            "   {tx}.delete(table_name=\"{table_name}\", "
            "   rowid={row_id})\n"
        )

        self.assert_template = (
            "assert {tx} is not None, '{tx} is not initialized, use BEGIN()'\n"
            "success = True\n"
            "if {tx}.get_status() == TransactionStatus.RUNNING:\n"
            "   if not {constraint}:\n"
            "       self.abort_transaction({tx})\n"
        )

        self.begin_template = (
            "assert {tx} is None, '{tx} is already initialized'\n"
            "{tx} = self.begin_transaction()\n"
            "success = True"
        )

        self.commit_template = (
            "assert {tx} is not None, '{tx} is not initialized, use BEGIN()'\n"
            "if {tx}.get_status() == TransactionStatus.RUNNING:\n"
            "   success = self.commit_transaction({tx})\n"
            "else:\n"
            "   success = True"
        )

        self.abort_template = (
            "assert {tx} is not None, '{tx} is not initialized, use BEGIN()'\n"
            "success = True\n"
            "if {tx}.get_status() == TransactionStatus.RUNNING:\n"
            "   self.abort_transaction({tx})"
        )

    def generate_code(self, schedule):
        """ Generate the executable code from given schedule

        Parameters
        ----------
            schedule :list
                The schedule of transactions represented as list of string
                ['TX1; val = READ(table_name=, ...)',
                ....]


        Returns
        -------
            (variables, tx_id, pseudo_code, gen_code)

                variables :set
                    A set containing all the variable names used by the stat-
                    ements. This is used to create the execution context

                tx_id :list
                    The list representing the transaction name which is
                    present in the given schedule at every step.

                pseudo_code :list
                    The list representing the pseudocode in every step of
                    given schedule.

                gen_code :list
                    The list of generated code corresponding to the pseu-
                    docode given in the schedule at every step
        """

        variables = set()
        tx_id = []
        pseudo_code = []
        gen_code = []
        statements = self._extract_statements(schedule)

        # Detect the type of functions used in every statement
        # and generate the code respectively.
        for statement in statements:
            tx_id.append(statement[0])
            pseudo_code.append(statement[1])
            if 'BEGIN' in statement[1]:
                gen_code.append(self._gen_begin(*statement))
            elif 'READ' in statement[1]:
                gen_code.append(self._gen_read(*statement, variables))
            elif 'UPDATE' in statement[1]:
                gen_code.append(self._gen_update(*statement))
            elif 'INSERT' in statement[1]:
                gen_code.append(self._gen_insert(*statement))
            elif 'DELETE' in statement[1]:
                gen_code.append(self._gen_delete(*statement))
            elif 'ASSERT' in statement[1]:
                gen_code.append(self._gen_assert(*statement))
            elif 'COMMIT' in statement[1]:
                gen_code.append(self._gen_commit(*statement))
            elif 'ABORT' in statement[1]:
                gen_code.append(self._gen_abort(*statement))

        return variables, tx_id, pseudo_code, gen_code

    def _extract_statements(self, schedule):
        """ Parse the string schedule and extract the statements, transaction
            name, and index for every statement in the schedule.

            Note: First line of the schedule must contain the transaction names

        Parameters
        ----------
            schedule :str
                The schedule in string format

        Results
        -------
            list of list representing  [name, idx, statement]
                name :str
                    The name of the transaction which contains this statement
                idx :int
                    The sequence number of this statement
                statement :str
                    The pseudocode statement
        """

        tx_statements = []

        for statement in schedule:
            tokens = statement.strip().split(';')
            assert len(tokens) == 2, 'Invalid statement: {}, expected \
                    2 tokens, received {}'.format(statement, len(tokens))

            tokens = [token.strip() for token in tokens]

            tx_statements.append(tokens)
        return tx_statements

    def _gen_begin(self, tx, statement):
        """ Generate code for begin statement

        Parameters
        ----------
            tx :str
                The name of the transaction containing this statement

            statement :str
                The pseudocode BEGIN statement

        Returns
        -------
            The generated code for the pseudocode.
        """
        parameters = {
                'tx': tx
                }

        return self.begin_template.format(**parameters)

    def _gen_read(self, tx, statement, variables):
        """ Generate code for read statement

        Parameters
        ----------
            tx :str
                The name of the transaction containing this statement

            statement :str
                The pseudocode READ statement

        Returns
        -------
            The generated code for the pseudocode.
        """
        parameters = re.findall(self.read_re, statement)
        assert len(parameters) == 1 and len(parameters[0]) == 4, """
            unable to parse READ_STATEMENT: \"{}\",\
                parsed_output: {}""".format(statement, str(parameters))

        assert parameters[0][0].isidentifier(), """
            Invalid variable name: {} in \"{}\"""".format(parameters[0][0],
                                                          statement)

        assert parameters[0][2].isdecimal(), """
            Invalid row_id: {} in \"{}\"""".format(parameters[0][2],
                                                   statement)

        variables.add(parameters[0][0])
        parameters = {
                'tx': tx,
                'var_name': parameters[0][0],
                'table_name': parameters[0][1],
                'row_id': int(parameters[0][2]),
                'column': parameters[0][3]
                }

        return self.read_template.format(**parameters)

    def _gen_update(self, tx, statement):
        """ Generate code for update statement

        Parameters
        ----------
            tx :str
                The name of the transaction containing this statement

            statement :str
                The pseudocode UPDATE statement

        Returns
        -------
            The generated code for the pseudocode.
        """
        parameters = re.findall(self.update_re, statement)

        assert len(parameters) == 1 and len(parameters[0]) == 3, """
            unable to parse UPDATE_STATEMENT: \"{}\",\
                parsed_output: {}""".format(statement, str(parameters))

        assert parameters[0][1].isdecimal(), """
            Invalid row_id: {} in \"{}\"""".format(parameters[0][1],
                                                   statement)

        #  up_dict = None
        #  try:
        #      up_dict = eval(parameters[0][2])
        #  except EOFError:
        #      assert False, "invalid dictionary encoding: {}\
        #              ".format(parameters[0][2])
        #
        #  assert isinstance(up_dict, dict)

        parameters = {
                'tx': tx,
                'table_name': parameters[0][0],
                'row_id': int(parameters[0][1]),
                'update': parameters[0][2]
                }

        return self.update_template.format(**parameters)

    def _gen_insert(self, tx, statement):
        """ Generate code for insert statement

        Parameters
        ----------
            tx :str
                The name of the transaction containing this statement

            statement :str
                The pseudocode INSERT statement

        Returns
        -------
            The generated code for the pseudocode.
        """
        parameters = re.findall(self.insert_re, statement)

        assert len(parameters) == 1 and len(parameters[0]) == 2, """
            unable to parse INSERT_STATEMENT: \"{}\",\
                parsed_output: {}""".format(statement, str(parameters))

        #  up_dict = None
        #  try:
        #      up_dict = eval(parameters[0][1])
        #  except EOFError:
        #      assert False, "invalid dictionary encoding: {}\
        #              ".format(parameters[0][1])
        #
        #  assert isinstance(up_dict, dict)

        parameters = {
                'tx': tx,
                'table_name': parameters[0][0],
                'insert': parameters[0][1]
                }

        return self.insert_template.format(**parameters)

    def _gen_delete(self, tx, statement):
        """ Generate code for delete statement

        Parameters
        ----------
            tx :str
                The name of the transaction containing this statement

            statement :str
                The pseudocode DELETE statement

        Returns
        -------
            The generated code for the pseudocode.
        """
        parameters = re.findall(self.delete_re, statement)

        assert len(parameters) == 1 and len(parameters[0]) == 2, """
            unable to parse INSERT_STATEMENT: \"{}\",\
                parsed_output: {}""".format(statement, str(parameters))

        assert parameters[0][1].isdecimal(), """
            Invalid row_id: {} in \"{}\"""".format(parameters[0][1],
                                                   statement)
        parameters = {
                'tx': tx,
                'table_name': parameters[0][0],
                'row_id': parameters[0][1]
                }

        return self.delete_template.format(**parameters)

    def _gen_assert(self, tx, statement):
        """ Generate code for assert statement

        Parameters
        ----------
            tx :str
                The name of the transaction containing this statement

            statement :str
                The pseudocode ASSERT statement

        Returns
        -------
            The generated code for the pseudocode.
        """
        parameters = re.findall(self.assert_re, statement)

        assert len(parameters) == 1, """
            unable to parse INSERT_STATEMENT: \"{}\",\
                parsed_output: {}""".format(statement, str(parameters))

        parameters = {
                'tx': tx,
                'constraint': parameters[0]
                }

        return self.assert_template.format(**parameters)

    def _gen_commit(self, tx, statement):
        """ Generate code for commit statement

        Parameters
        ----------
            tx :str
                The name of the transaction containing this statement

            statement :str
                The pseudocode COMMIT statement

        Returns
        -------
            The generated code for the pseudocode.
        """
        parameters = {
                'tx': tx
                }

        return self.commit_template.format(**parameters)

    def _gen_abort(self, tx, statement):
        """ Generate code for abort statement

        Parameters
        ----------
            tx :str
                The name of the transaction containing this statement

            statement :str
                The pseudocode ABORT statement

        Returns
        -------
            The generated code for the pseudocode.
        """
        parameters = {
                'tx': tx
                }

        return self.abort_template.format(**parameters)

    def get_read_write_set(self, statement):
        parameters = None
        rw_set = None

        if 'READ' in statement:
            parameters = re.findall(self.read_re, statement)
            rw_set = (parameters[0][2], 'r')
        elif 'UPDATE' in statement:
            parameters = re.findall(self.update_re, statement)
            rw_set = (parameters[0][1], 'w')
        elif 'DELETE' in statement:
            parameters = re.findall(self.delete_re, statement)
            rw_set = (parameters[0][1], 'w')

        return rw_set
