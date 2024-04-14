"""

File formats and compression techniques:
========================================

Why do we need different file formats:

        - we want to save storage
        - we want tom make faster process
        - we want less time for I/O operations

So, there are lot more file formats available for choose.

The below are the consideration tp select the right formats.

    -> Faster Reads
    -> Faster Writes
    -> Splittable
    -> Schema evolution support
    -> Should support advanced compression techniques...etc

The file formats has been divided into two parts.

        1. Row base
        2. Column base

Row base file data storage:
---------------------------
1, 2023-07-23 00:00:00, 11956, CLOSED | 2, 2023-04-28 00:00:00, 254756, PENDING | 3,2024-01-26 00:00:00,124356,CLOSED

    -> Easy for Faster writes.
    -> which means we can easily add new rows of data.
    -> If we need to read subset of columns with query, then we have to read the entire row. So it takes time.
    -> If we go with "select * from table" then it is fine.
    -> If there will be a huge number of columns, then read subset of columns is not efficient.
    ->

ex: csv

Column Base file data storage:
------------------------------
1,2,3 | 2023-07-23 00:00:00,2023-04-28 00:00:00,2024-01-26 00:00:00 | 11956,254756, 124356 |CLOSED, PENDING, CLOSED

    -> Efficient in reads.
    ->

ex: parquet


when we talk about csv format:
------------------------------
    -> storage(required more space)
    -> Processing is slow.
    -> I/O operations takes more time.
    -> by default it will create dataframe with string for all the columns. So if any long integer columns available
        then it will consume additional space for saving in integer.

XML and Json:
-------------
    -> what ever mentioned above is same with these formats as well, but having some more issues.
    -> these two formats are not splittable. any non-splittable file formats are not recommend for big data.
    -> if the data of each record saved in individual rows in json file than only it will be splittable.
    -> files size are bulky.
    ->

Specialized file formats:
-------------------------

        1. Parquet
        2. ORC
        3. AVRO


"""