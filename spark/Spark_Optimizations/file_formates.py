"""

File formats and compression techniques:
========================================

Why do we need different file formats:

        - we want to save storage
        - we want to make faster process
        - we want less time for I/O operations

So, there are lot more file formats available for choose.

The below are the consideration to select the right formats.

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

- Snappy is the default compression technique for all the file formats.

ex: csv

Column Base file data storage:
------------------------------
1,2,3 | 2023-07-23 00:00:00,2023-04-28 00:00:00,2024-01-26 00:00:00 | 11956,254756, 124356 |CLOSED, PENDING, CLOSED

    -> Efficient in reads.
    -> There is a flexibility to read subset of columns from all the columns.

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

        1. Parquet - column base - splittable
        2. ORC (Optimized row columnar)- column base - splittable
        3. AVRO - row base - splittable

-All the above formats are splittable and supports schema evolution.
-We can use any kind of compression technique with the above formats.
-Metadata also embedded with data and compression codec also mentioned in metadata.


* Avro:
        - It is row based format, will support faster writes but slower reads.
        - Avro can be fit for landing layer of the data lake because we will always read all the columns in this area.
        - If we will open the file, metadata can be readable but data will be displayed as binary format.

* ORC and parquet:
        - Column based file formats.
        - Not efficient for writes.
        - Optimized for reads.
        - Highly efficient for storage.
        - We can use any type of compression easily why because same type of data stores togather.
        - ORC best with HIVE
        - Parquet is default and best with spark.

    -> Parquet:
        - format contains like header, body , tail.
        - header is like name
        - body contains - row groups -> column chunks -> pages
        - footer - metadata
        - lets consider one folder is having 4 parquet files.
            - one file is with 500mb - 80,000 rows

                - 1st row group - 20,000
                    column chunk1 - order_id
                    column chunk2 - order_date
                    column chunk3 - customer_id
                    column chunk4 - order_status

                - 2nd row group - 20,000
                    same as above

                - 3rd row group - 20,000
                    same
                -4th row group - 20,000
                    same

    here each column chunk is having actual data along with metadata like total count, min and max count like.

    So, a parquet file  -> row groups (128mb generally) -> column chunks -> pages with some metadata.
                            also having some metadat like pages.


"""