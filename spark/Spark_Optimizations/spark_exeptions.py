
"""

parseException:
==============

        - It will throw while having syntactical errors.

        > spark.sql("select * fro orders_table")

        - here the error at 'fro' because actual keyword is 'from'
        - so, spark will check the syntactical level of the code, than spark will analyse the code.
        - so, here parseException will throw.


AnalysisException:
==================

        - spark will throw this error while chcking the column names, table names and database names.
        - If any of the above is not matching than only this exception will throw.

        > spark.sql("select * from orde3rs")

        - here code is syntactically perfect so, it passed the parseException.
        - Now, spark will check the columns and tables and here table name is not existing with the mentioned name.
        - then AnalysisException will throw like: Table or View not found: orde3rs: line 1.


"""