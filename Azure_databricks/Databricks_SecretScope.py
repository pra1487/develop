"""
Secret Scope:
=============

There are two ways to manage secrets.

        1. Azure key vault backed secret scope: - Recommended
        2. Databricks backed secret scope:


1. Azure key vault backed secret scope: - Recommended
---------------------------------------
     - keep the keys in azure key vault and connect with databricks secret scope.
     - Any other Azure resources can also able to use the keys while keeping the keys in Azure key vault.
     - That is way, this is recommended.

Process:
--------

    Azure Key vault:
    ----------------
    - create key vault service
    - set both permissions in Access Control (IAM)

        * one is for user - for add or remove secrets in key vault like owner
        * another one is for databricks - to access the secrets from databricks.

    - process to add user:

            Open key vault service --> click on "Access Control (IAM)"
                                    --> click on "Add"
                                    --> select "Add role assignment"
                                    --> search with 'key vault' in search bar in job "function roles"
                                    --> select "Key Vault Secrets Officer" can perform any action on key vault service
                                    --> click on 'Next'
                                    --> click on '+ Select members'
                                    --> choose member email on new panel opened on right side
                                    --> click on 'select' on same panel
                                    --> click on 'review + assign'

    - Without assigning this role, we can not able to add secrets in key vault service

    - Process to add databricks:

            Open key vault service --> click on "Access Control (IAM)"
                                    --> click on "Add"
                                    --> select "Add role assignment"
                                    --> search with 'key vault' in search bar in job "function roles"
                                    --> select "Key Vault Administrator" bigger level permission.
                                        or
                                        select "Key Vault Secret User" - Read secret contents. only works for key vaults
                                        'Azure role-based access control' -- recommended for databricks.
                                    --> click on 'next'
                                    --> click on '+ Select members'
                                    --> Type AzureDatabricks in search bar in which opened on right side panel
                                    --> click on 'select' on same panel
                                    --> click on 'review + assign'
Now creating secrets:
--------------------
    - Process:
            Open key vault --> click on secrets in setting section.
                            --> click on "Generate/import"
                            --> enter the details as shown in below
                                    * Upload option = 'manual' is fine
                                    * Name = 'storage_dev_key' (Our choice)
                                    * Value = (secret key)
                                    * Content type =
                                    * Set activation date =
                                    * Set expire date =
                                    * Enabled =
                            --> click on "Create"

Now go to databricks to add this secret key:
--------------------------------------------
  - There is a hidden scope available in databricks
  - we can able to store many secrets.

  - Process:
              --> type "#secrets/createScope" in the url of the databricks to navigate the panel
              --> enter Scope Name ex. databricks_secretscope_dev
              --> enter DNS name
              --> enter resource ID
                    --> go to 'key vault' resource
                    --> click on 'properties'
                    --> copy the 'Vault URL'
                    --> copy the 'Resource ID'
              --> click on 'create'


Now we can check with dbutils:
    - dbutils.secrets.get('databricks_secretscope_dev', 'storage_dev_key')
        out: "[REDICATED]"

Now we can use this in mount command:

    -dbutils.fs.mount(source = 'wasbs://input_datasets@pp_storage_dev.blob.core.windows.net',
                    mount_point = '/mnt/retaildb',
                    extra_configs = {'fs.azure.account.key.pp_storage_dev.blob.core.windows.net':
                    dbutils.secrets.get('databricks_secretscope_dev','storage_dev_key')})

    - df = spark.read.csv('/mnt/retaildb/orders.csv', header=True)
    - df.show()



2. Databricks backed secret scope:
----------------------------------
    - Databricks don't take azure support.
    - Keys are keeping with encrypted databricks database


"""