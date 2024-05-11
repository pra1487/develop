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

2. SAS Key:
-----------
    -> We can able to control container level access.
    -> We have better security level than Access Key


3. Service Principle:
---------------------
    -> We can able to control folder/directory level access.
    -> More security than SAS.


"""