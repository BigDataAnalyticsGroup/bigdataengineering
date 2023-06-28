from django.db import connection
import sqlparse
from django.db import reset_queries


def clean_queries():
    connection.queries.clear()
    reset_queries()


# shows the queries from the connection and pretty-prints them
def show_queries(number_of_queries_only=False):
    ret = ""
    if number_of_queries_only:
        ret += "number of queries:", len(connection.queries)
        return

    count = 0
    for q in connection.queries:
        ret += "****************************************************************\n"
        ret += "Query " + str(count) + ":" + "\n"
        ret += sqlparse.format(q['sql'], reindent=True, keyword_case='upper') + "\n"
        count += 1
        ret += "\n"
        # execute the query again, but this time with cursor:
        ret += "Result:\n"
        with connection.cursor() as cursor:
            cursor.execute(q['sql'])
            for row in cursor:
                ret += str(row) + "\n"

    ret += "****************************************************************\n"
    ret += "number of queries: " + str(len(connection.queries)//2) + "\n"
    ret += "****************************************************************\n"
    return ret
