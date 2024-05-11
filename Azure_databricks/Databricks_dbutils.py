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

dbutils.notebook.run('/users/trendytech.sumit@outlook.com/childnotebook',60)


widgets utility:
----------------
                    dbutils.widgets.help()
        - combobox -->
        - dropdown
        - multiselect
        - text

dbutils.widgets.combobox(name='order_status', defaultValue='CLOSED', choices=['CLOSED','COMPLETE','PROCESSING',]
                        label='ORDER STATUS')
    -> we can either select a value from the existing dropdown or can type in your value.

How to use this:

                df = spark.read.csv('dbfs:/FileStore/retaildata/orders.csv', header=True)
                os = dbutils.widgets.get('order_status')

                df.where("order_status == '{}'".format(os)).show()

                # this will filter the df.


dbutils.widgets.dropdown(name='orderstatus', defaultValue='CLOSED', choices=['CLOSED','COMPLETE','PROCESSING',]
                        label='ORDER STATUS')
    -> we can not able to enter/type the required text, only need to select from dropdown list.


dbutils.widgets.multiselect(name='orderstatus', defaultValue='CLOSED', choices=['CLOSED','COMPLETE','PROCESSING',]
                        label='ORDER STATUS')
    -> we can select more than one dropdown list.

dbutils.widgets.text(name='orderstatus',defaultValue='CLOSED', label='ORDER STATUS')
    -> we can just enter/type any value as per our requirement


dbutils.widgets.remove('orderstatus')
dbutils.widgets.removeAll()





"""