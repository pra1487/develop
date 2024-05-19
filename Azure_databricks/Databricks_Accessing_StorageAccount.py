"""
Accessing Storage Account:
==========================

    There are three ways to access storage account from databricks:

        1. Access Key / Account Key (Priority-3)
        2. SAS Key (Shared Access Signature) (Priority-2)
        3. Service Principle - (Priority-1)

1. Access Key:
 -------------
    -> User can able to access all the storage account including all the containers.
    -> we can not restrict the user for some particular containers.
    -> check in details in "Databricks_SecretScope.py".
    -> Access kay option is availabe in only storage account level.

2. SAS Key:
-----------
    -> We can control container level access.
    -> We have better security level than Access Key
    -> There is no Access Key option available in container level.

    -Process:
        -> Go to storage resource
        -> click on "Shared access signature" under the "Security + networking"
        -> select Allowed services, Allowed resource types.
        -> click on "Generate SAS connection string"
        -> copy the SAS token

    Accessing in code:
    ------------------

    spark.conf.set("fs.azure.account.auth.type.ttstorageaccount102.dfs.core.windows.net",'SAS')
    spark.conf.set("fs.azure.sas.token.provider.type.ttstorageaccount102.dfs.core.windows.net,
    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set("fs.azure.sas.fixed.token.ttstorageaccount102.dfs.core.windows.net",
    "<SAS token>")

    df = spark.read.csv('abfs://retaildb@<storageaccount>/raw/order.csv', header=True)
    df.show()


3. Service Principle:
---------------------
    -> We can able to control folder/directory level access.
    -> More security than SAS.

    -Process:
        -> go to "Azure active directory"
        -> click on "app registrations"
        -> click on "New registration"
        -> Enter name - our choice ex. databricks-new-sp
        -> click ok "register" --> after registration complete
        -> copy all three (Application (client) ID, Object ID, Directory(tenant) ID) from that opened page
            * upto now, in simple language we have created a 'username'
            * now create 'password'
        -> click on "certificates & secrets"
        -> click on "+ New client secret" -> Add a client secret panel open on right side
        -> Enter description ex. sp-new-client-secret\
        -> click on "Add"
        -> copy the "value" after creating new client secret.
            * service principle is created.
            * now we can give access to storage account
        -> go to storage account
        -> click on "Access control (IAM)"
        -> click on 'Add' --> select "Ad role assignment"
        -> search with 'Storage Blob Data Contributor' in search bar under 'Role'
        -> click on 'view' on extream right of the role. --> click on 'select role'
        -> click on "+Select members"
        -> select our service principle from there
        -> click on 'select' -> click 'next' -> click on 'next'
        -> click on 'Review + Assign'

Access in code:
----------------

Template:
---------
spark.conf.set("fs.azure.account.auth.type.<storage account>.dfs.core.windows.net","OAuth")

spark.conf.set("fs.azure.account.oauth.provider.type.<storage account>.dfs.core.windows.net",
"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

spark.conf.set("fs.azure.account.oauth2.client.id.<storage account>.dfs.core.windows.net",
"<application id>")

spark.conf.set("fs.azure.account.oauth2.client.secret.<storage account>.dfs.core.windows.net",
"<secret>")

spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage account>.dfs.core.windows.net",
"https://login.microsoftonline.com/<Directory ID>/oauth2/token")

---------- adding creds

spark.conf.set("fs.azure.account.auth.type.ttstorageaccount102.dfs.core.windows.net","OAuth")

spark.conf.set("fs.azure.account.oauth.provider.type.ttstorageaccount102.dfs.core.windows.net",
"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

spark.conf.set("fs.azure.account.oauth2.client.id.ttstorageaccount102.dfs.core.windows.net",
"9f178d90-1bb3-43dc-b60f-51552e64e418")

spark.conf.set("fs.azure.account.oauth2.client.secret.ttstorageaccount102.dfs.core.windows.net",
"pQV8Q~VXv9CLGePn.ER3M51tMb-lNsygcG-fVbrC")

spark.conf.set("fs.azure.account.oauth2.client.endpoint.ttstorageaccount102.dfs.core.windows.net",
"https://login.microsoftonline.com/a216c39d-a41f-4225-8bca-11c3f51bb637/oauth2/token")

df = spark.read.csv.('abfs://retaildb@ttstorageaccount102.dfs.core.windows.net/raw/orders.csv',header=True)
df.show()

"""