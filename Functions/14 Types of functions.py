# Types of Functions

"""
1. Lambda Function or Anonymous function
2. Filter Function
3. Map Function
4. Reduce Function
5. Boolean Function
6. Recursive Function
"""

# 1. Lambda Function: A Function without ay name is called lambda function
# So, we can assign this functon to a variable then that variable act as a function name
# syntax for defining lambda:
# lambda arguments : expression

#ex: lambda function to squere a number

sq = lambda x:x**2
print(type(sq))
print(sq(20))
print(sq(10))

# same task in normal func

def sq1(x):
    return x**2
print(sq1(10))

# In which situation we go with lamda function.
# ans: when we providing one funcation as an argument to another function then only we will go with lambda.

# ex:
"""
def f1(function):
    --------------
    --------------
    --------------

def f2():
    -------------
    -------------
f1(f2())

"""
# Note: We can specify multiple args in lambda but only one expression.

p = lambda x,y:x*y # here x,y are args x*y is expression
print(p(10,20)) # non-kwargs
print(p(x=20,y=4)) # kwargs

q = lambda x=10,y=20:x*y # here x,y are default args
print(q())
print(q(20,y=30)) # non-d and default args

r = lambda x,*y: sum(y)
r1 = r(10,20,30,40)
print(r1)

