"""
Databricks Filesystem (dbfs):
=============================

* DBFS is the wrapper on the azure blob, ADLS GEN2
* Root of the DBFS is /FileStore/
* We can mount or unmount the storage with this DBFS

* DBFS is a distributed file system mounted into to a databricks.
* We can not able to browse the DBFS by default. we have to enable as below.

            Databricks workspace settings --> Admin Console --> Workspace Settings --> DBFS File Browser

    Then only, we can able to see the DBFS under the Data tab.

"""