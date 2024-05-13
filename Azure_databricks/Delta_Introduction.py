"""
What is a datalake.?
====================

    - A data lake is a storage repository that holds a vast amount of raw data in its native format.
    - Data lake stores all the data in the form of files.
    - Data lake can hold any kind of data.
    ex. aws s3, ADLS gen2 and GCS

  Advantages:
    - cost effective which means charge very less.
    - scalable
    - supports any kind of data (structured, semi structure and unstructured)

  Challenges:
    - ACID guarantees (Atomicity Cnsistency Isolation Durability)
    - when ever we are doing day to day transaction process we required ACID.
    - Any database provides ACID guarantees.
    - Atomicity: ALl or None
        10k will be decuting from you account
        your friend account will be added with 10k
    - Consistence:

    - A jobs is failing while appending the data some outfiles will generate, if we will re-run the job
        previously generated files also appended newly.
    - A job is failing while overwriting the data
        -> first deletes the existing data
        -> next place the new data.
        So job may fail after deleting the existing data than we will lost the existing data before load the new data.
    - Append with new schema (2 new cols) then the newly appended part files only having the new columns.
    - Difficult to maintain historical versions.

    To handle all of these issues, we will use DELTA lake.


Delta lake:
===========
Some improvements on top of datalake to overcome the above challenges.
Delta lake is an open source storage layer
It is small utility installed on your spark cluster.

Parquet file format:
    - It is column based file format
    - very well used with spark
    - It is embeded with metadata.

df.write.format('parquet')

 --------------------------------------
 | Delta = parquet + Transaction logs |
 --------------------------------------

df.write.format("delta")

    - If this rnas with 5 tasks then 5 parquet files will be generated in output location.
    - There will be another file which is named with "_delta_logs".
    - There will be 00000.json file will be create in inside the _delta_logs folder.
    - This .json file is holding the transaction details which are add, delete, update like this.

operation-1 write:
-----------------
part-000.parquet
part-001.parquet
part-002.parquet
part-003.parquet
part-004.parquet
part-005.parquet

_delta_logs
00000.json

    - This json log files will be create only after the job will complete succcess.
    - If the job will fail in intermediate state than the log file won't be create.

Operation-2 append:
-------------------
part-006.parquet
part-007.parquet

_delta_log
00001.json

    - Two more files will be append to te target location
    - now total 7 part files are generated.
    - one more json log file will be create
    - If the append process will fail than the json log file won't be create.

Operation-2 one more append:
---------------------------
part-008.parquet

_delta_log
00003.json

    - Now total 8 part files are generated
    - 3 json log files are generated.

for read process:
-----------------
    -> spark will read first json log files.
    -> read the required part files based on the json log file.

DML operations:
---------------

updates:
--------
    data files:
                part-000.parquet
                part-001.parquet
                part-002.parquet
                part-003.parquet
                part-004.parquet
    _delta_log:
                00000.json
                -----------
                add part-000.parquet
                add part-001.parquet
                add part-002.parquet
                add part-003.parquet
                add part-004.parquet

emp_id, emp_name, salary -> are the data cols in parquet files.
101, kohli, 10000
102, mahi, 11000
101, shoni, 12000

update table set salary = 15000 where emp_id = 101;
Than new part file will create with new change
New json log file also create.

part-005.parquet
----------------
101, kohli, 15000
102, mahi, 11000
101, shoni, 12000

    new data files:
                part-001.parquet
                part-002.parquet
                part-003.parquet
                part-004.parquet
                part-005.parquet
    _delta_log:
        00001.json:
                    add part-005.parquet
                    remove part-000.parquet

Same thing happened for delete records as well.

When we will read the data from this logfile, we will get the updated data.

- Schema changes also taken care
- Version history also.
-
"""