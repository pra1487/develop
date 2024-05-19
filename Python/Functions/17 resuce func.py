# reduce(): for cummulative operations

# reduce() function also takes two arguments
#           1. lambda function
#           2. Iterable type like list

# ex:1

from functools import reduce # need to import this for reduce

list1 = [1,2,3,4,5]
res = reduce(lambda x,y: x+y, list1)
print(res)

# ex:2 find the laargest number in list

res2 = reduce(lambda x,y: x if x>y else y, list1)
print(res2)
