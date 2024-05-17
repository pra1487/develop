"""
Mount and UnMount:
====================

mount()
--------        dbutils.fs.help('mount')

storage account name : pp_storage_dev
container name : input_datasets
files: orders.csv, customer.csv

to mount: dbutils.fs.mount(source, mount_point, extra configs)

    source = 'wasbs://input_datasets@pp_storage_dev.blob.core.windows.net'
    mount_point = '/mnt/retaildb'
    extra_configs = {'fs.azure.account.key.pp_storage_dev.blob.core.windows.net':'accountkey'}

dbutils.fs.mount(source = 'wasbs://input_datasets@pp_storage_dev.blob.core.windows.net',
mount_point = '/mnt/retaildb',
extra_configs = {'fs.azure.account.key.pp_storage_dev.blob.core.windows.net':'accountkey'})

dbutils.fs.ls('/mnt')
dbutils.fs.ls('/mnt/retialdb/')

df = spark.read.csv('/mnt/retaildb/orders.csv',header=True)
df.show()


unmount()
---------       dbutils.fs.unmount('/mnt/retaildb')

        - This will unmount the retaildb which we did mount earlier

- To see all the mount points
dbutils.fs.mounts()

- to update the existing mount
dbutils.fs.updateMount('/mnt/retaildb', '/mnt/retail_new', True)

-
"""