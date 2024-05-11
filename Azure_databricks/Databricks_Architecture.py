"""

Databricks Architecture:
========================

All the databricks resources are available under two subscriptions.

    1. Control plane - Databricks subscription - It will create along with workspace.
    2. Data place - Our own azure subscription.

Control place:
--------------
                deploying in databricks subscription while creating the databricks workspace
                which are controlled by databricks not by us.

                - Databricks UI, cluster manager, dbfs, cluster metadata are deployed in db subscription.

Data place:
-----------
                In azure subscription

                - vnet, nsg, azure blob storage




"""