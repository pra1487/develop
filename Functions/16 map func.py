# map():

# map function takes two arguments same as like filter
#   1. lambda function
#   2. Iterable type like list

# ex:
list1 = [10,20,30,40,50]

# Adding 5 to each element in list

add_li = list(map(lambda x: x+5, list1))
print(add_li, "\n")

res = list(map(lambda x:x**2 if x%2==0 else x**3, list1))
print(res)

emps = [[101,"miller",30000,"m",11,"hyd"],
        [102,"blake",40000,'m',12,'Pune'],
        [103,"Sony",50000,'f',11,"Pune"],
        [104,"Sita",60000,'f',12,'hyd'],
        [105,'virat',70000,'m',13,'Delhi']]

# convert all the emp names to upper case
res1 = list(map(lambda x: [x[0],x[1].upper(),x[2],x[3],x[4],x[5]], emps))
print(res1, "\n")


# adding 5k for each emp as bonus
res2 = list(map(lambda x:[x[0],x[1],x[2]+5000,x[3],x[4],x[5]],emps))
print(res2,"\n")

# Convert each emp name stats with upper case
res3 = list(map(lambda x: [x[0],x[1].capitalize(),x[2],x[3],x[4],x[5]],emps))
print(res3)

# m --> male and f--->female
res4 = list(map(lambda x:[x[0],x[1],x[2],"male" if x[3]=='m' else 'female',x[4],x[5]],emps))
print(res4,"\n")

# add two lists using map
even = [2,4,6,8]
odd = [1,3,5,7]

res5 = list(map(lambda x,y: x+y, even,odd))
print(res5)


