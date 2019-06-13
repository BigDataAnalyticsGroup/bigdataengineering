""" This module extends the ra.Relation class to allow row_id based get, put
    and delete operations of the tuples.

    The tuples are stored as a list of list, where the outer list stores a
    single list per unique rowid, and the inner list stores multiple versions
    of the corresponding rowid.

    The most recent version is appended to the end of the list so the most
    recent version of tuple with row_id 0 can be accessed using:
        self.tuples[0][-1]

    Please note that multiple versions are stored only if the Table class
    object is instantiated using the use_multiversion option set to True.

    The delete operation on any row_id inserts the row_id into the
    deleted_row_ids list, which is checked for a free row_id in the insert
    operation. During delete operation, the corresponding list holding the
    tuple is made empty leaving hole in the self.tuples list. This empty
    row is ingored while printing the table.
"""

from ra.relation import Relation


class Table(Relation):
    """ Implementation of Table class which extends the existing ra.relation.
    Relation class.

    Attributes
    ----------
        schema :list
            Schema of the table as a list of pair
            ('attribute_name', attribute_type)

        tuples :list
            List which maintains multiple tuple versions for every row

        deleted_row_ids :list
            List of :int rowids whose tuples were deleted.

        use_multiversion :bool
            Whether to store multiple versions per tuple or not
    """

    def __init__(self, name, schema, use_multiversion=False):
        """ Instantiates an object of the Table class

        Parameters
        ----------
            name :string
                Name of the relation or expression the relation object is built
                from.

            schema :list
                Schema of the table as a list of pair
                ('attribute_name', attribute_type)

            use_multiversion :bool
                Whether to store multiple versions per tuple or not
        """
        super().__init__(name, schema)

        self.tuples = []
        self.schema = schema
        self.deleted_row_ids = []
        self.use_multiversion = use_multiversion

    def get(self, rowid):
        """ Get a tuple with the given row_id

        Parameters
        ----------
            rowid :int
                rowid of the tuple

        Returns
        -------
            the row as a tuple of attribute values
        """
        if rowid < 0 or rowid >= len(self.tuples):
            return []

        return self.tuples[rowid]

    def put(self, row):
        """ Insert a row into the table

        Parameters
        ----------
            row :list
                List of attribute values in a correct order
        """
        rowid_index = self.get_attribute_index('row_id')
        assert row is not None
        assert row[rowid_index] >= 0 and row[rowid_index] < len(self.tuples)

        row = tuple(row)
        self._check_schema(row)
        if not self.use_multiversion:
            self.tuples[row[rowid_index]] = [row]
        else:
            self.tuples[row[rowid_index]].append(row)

    def get_next_row_id(self):
        """ Get the next free rowid where the row can be inserted

        Returns
        -------
            :int The free rowid where the row can be inserted
        """
        if len(self.deleted_row_ids) > 0:
            # is there an existing deleted row?
            rowid = self.deleted_row_ids.pop()
        else:
            rowid = len(self.tuples)
            # append an empty row
            self.tuples.append([])
        return rowid

    def tuple_to_dict(self, row):
        """ Convert the tuple into a dictionary for single version store

        Parameters
        ----------
            row :list
                list of values in the tuple

        Returns
        -------
            tuple_dictionary :dict
                Dictionary with the tuple mapping attribute-name to value
        """
        assert len(row) == len(self.attributes)
        ret_dict = {}
        for i in range(len(self.attributes)):
            ret_dict[self.attributes[i]] = row[i]

        return ret_dict

    def dict_to_tuple(self, update_dict, old_row):
        """ Convert the given dictionary into a tuple for single version store

        Parameters
        ----------
            update_dict :dict
                Dictionary with the tuple mapping attribute-name to value

            org_tuple :list
                copy of the most recent original tuple

        Returns
        -------
            row :tuple
                Properly ordered tuple representation of the given dictionary
        """
        old_row = list(old_row)
        for attribute_name in update_dict:
            if attribute_name in self.attributes:
                index = self.get_attribute_index(attribute_name)
                old_row[index] = update_dict[attribute_name]
            else:
                return None
        return tuple(old_row)

    def delete(self, rowid):
        """ Delete the given tuple from the table

        Parameters
        ----------
            rowid :int
                The rowid which is to be deleted
        """
        assert rowid >= 0 and rowid < len(self.tuples)
        if rowid not in self.deleted_row_ids:
            self.tuples[rowid] = []

    def _print(self, limit, tuples, version=False):
        """ Prints the given table and its tuples in a tabular layout

        Parameters
        ----------
            limit :int
                The maximum number of rows to print
            version :bool
                If the table represents the version table. This adds
                additional text to the table name to make it distin-
                -guisable from the original table.
        """
        assert (limit is None or limit > 0)

        # calculate column width for printing
        col_width = self._get_col_width()
        # relation name bold
        if version:
            target = '-'*len(self.name) + '\n' \
                     '\033[1m' + self.name + ' (older_versions)' \
                     '\033[0m \n'
        else:
            target = '-'*len(self.name) + '\n' \
                     '\033[1m' + self.name + '\033[0m \n'

        # attribute names bold
        target += '-' * max(len(self.name), col_width * len(self.attributes))
        target += '\n' + '\033[1m' + ''.join(attr_name.ljust(col_width)
                                             for attr_name in self.attributes)
        target += '\033[0m \n'
        target += '-' * max(len(self.name), col_width * len(self.attributes))
        target += '\n'
        # tuples
        limitCount = 0
        for tup in tuples:
            if len(tup) > 0:
                target += ''.join(str(attr_val).ljust(col_width)
                                  for attr_val in tup)
                target += '\n'

                limitCount += 1
                if limit is not None and limitCount >= limit:
                    target += "\nWARNING: skipping "
                    target += str(len(self.tuples)-limit)
                    target += " out of " + str(len(tuples)) + " tuples..."
                    break
        print(target)

    def print_table(self, limit=None):
        """ Print the table, and a secondary version table (if exists)

        Parameters
        ----------
            limit :int
                The maximum number of rows to print
        """
        old_version_tuples = []
        most_recent_tuples = []

        for row in self.tuples:
            if len(row) > 0:
                most_recent_tuples.append(row[-1])

                for version in row[:-1]:
                    old_version_tuples.append(version)

        self._print(limit, most_recent_tuples, False)

        if len(old_version_tuples) > 0:
            print('\n\n')
            self._print(limit, old_version_tuples, True)

    def _get_col_width(self):
        """ Computes the maximum column width required to represent the
            relation in tabular layout.

        Returns
        -------
            :int The maximum column width required by the table
        """
        attr_name_width = max(len(attr_name) for attr_name in self.attributes)
        attr_val_width = max((len(str(attr_val))
                             for tup in self.tuples
                             for version in tup
                             for attr_val in version), default=0)
        return max(attr_name_width, attr_val_width) + 2  # padding
