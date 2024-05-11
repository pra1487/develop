"""

dbutils:(dbutils.help())
========================

There are many utilities are available under the dbutils for handling different zones.
for example:
            credentials - DatabricksCredentialsUtils
            data - DataUtils
            fs - DbFsUtils
            jobs - JobsUtils
            widgets - WidgetsUtils

-> we can check the available option under the fs utils with 'dfutils.fs.help()'.
-> we can check the fs commands functionality with 'dbutils.fs.help('cp')'.

File system utils:
-------------------     dbutils.fs.help()
                    cp, head, rm, mkdirs, ls, mv
        - cp  -> copy file from one location to another location.
        - head -> head will gove you 65536 bites from the file or we can mention the 50 or 70 or what ever.
        - rm -> remove file
        - mkdirs -> create directory
        - mv -> move file (copy to destination and delete the source file)
        - put ->


dbutils.fs.ls('/FileStore/') or dbutils.fs.ls('dbfs:/FileStore/') or %fs ls /

    [(path='dbfs:/FileStore/data', name='data/', size=0, modificationTime=),
    (path='dbfs:/FileStore/tables', name='tables/', size=0, modificationTime=)]

dbutils.fs.head('FileStore/retaildata/orders.csv',100)
or
%fs head dbfs:/FileStore/retaildata/orders.csv

dbutils.fs.mkdirs('/FileStore/temp5')

dbutils.fs.mv("/FileStore/temp5", "FileStore/temp6", True)

dbutils.fs.cp("Source", "Destination")

dbutils.fs.rm()


Data utils:
-----------
            dbutils.data.help()

            - dbutils.data provide the utils for understanding and interpreting datasets
            - summarize() Summarize a spark dataframe and visualize the statistics to get quick insights.

dbutils.data.help('summarize')

dbutils.data.summarize(df)


Notebook utility:
-----------------
                    dbutils.notebook.help()
        - exit(value:string) : This method lets you exit a notebook with value.
        - run(path: string, timeoutSeconds:int, arguments:Map) : This method runs a notebook and returns its exit value.








"""