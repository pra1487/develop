# filter(): for filtering data based on condition.
# it takes two args.
#       1. lambda function
#       2. Iterable type like list.

# ex:

list1 = [1,2,3,4,5,6]

# Task: filter those greater than 2
res = list(filter(lambda x:x>2,list1))
print(res,type(res))


# ex:

emps = [[101,"miller",30000,"m",11,"hyd"],
        [102,"blake",40000,'m',12,'Pune'],
        [103,"Sony",50000,'f',11,"Pune"],
        [104,"Sita",60000,'f',12,'hyd'],
        [105,'virat',70000,'m',13,'Delhi']]

# Task1: Filter those emps who belongs to hyd

hyd_res = list(filter(lambda x: x[5]=="hyd", emps))
print(hyd_res, "\n")

# filter male details

male_res = list(filter(lambda x: x[3]=="m", emps))
print(male_res, '\n')

# filter those whose above salary 40000

res1 = list(filter(lambda x: x[2]>40000, emps))
print(res1, "\n")

# we can also provide multiple conditions
res2 = list(filter(lambda x: (x[4]==12 and x[5]=="hyd"), emps))
print(res2)
