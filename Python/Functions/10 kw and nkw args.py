# Keyword args and non-keyword args:

# kw args: During function call, passing the values with parameter(keyword) name.
# nkw args: During function call, passing values without parameter (keyword) name.

def customer(cid,cname,bal,city="hyd"):
    print(cid,cname,bal,city)

customer(101,"prasad",60000,"Pune") # Nkwargs
customer(cname="Kumar",cid=102,bal=50000,city="CHN") # kwargs
customer(103,"Blake",bal=10000,city="pune") # nonkw and kwargs
#customer(cid=104,cname="Virat",70000,"Delhi")

# Note: After keyword args, we cannot specify non=kwargs
# nut after non-kwargs, we can able to specify kwargs


