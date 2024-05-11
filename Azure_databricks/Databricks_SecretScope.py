"""
Secret Scope:
=============

1. Azure key vault backed secret scope: - Recommended
---------------------------------------
     - keep the keys in azure key vault and connect with databricks secret scope.
     - Any other Azure resources can also able to use the keys while keeping the keys in Azure key vault.
     - That is way, this is recommended.


2. Databricks backed secret scope:
----------------------------------
    - Databricks don't take azure support.
    - Keys are keeping with encrypted databricks database


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
                                        select Key Vault Secret User - Read secret contents. only works for key vaults
                                        'Azure role-based access control' -- recommended for databricks.
                                    --> click on 'next'
                                    --> click on '+ Select members'
                                    --> Type AzureDatabricks in search bar in which opened on right side panel
                                    --> click on 'select' on same panel
                                    --> click on 'review + assign'


"""