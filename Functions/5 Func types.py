# Function  categories:
#-----------------------

# 1. Function without return type and without arguments.

def add1():  # No arguments
    x = 10
    y = 20
    z = x+y
    print(z)   # without return

add1()

# ----------------------------------------
#2. Functions with arguments and without return type

def add2(x,y):  # with arguments
    z = x+y
    print(z)    # without return

add2(3,4)

# -----------------------------------------------------------
#3. Functions with args and with return type

def add3(x,y):
    z = x+y
    return z  # What ever the function returns will be stored by the function call

print(add3(2,3)) 
#or
res = add3(2,3)
print(res)

# Advantage of return statement: If we will need to use the computed result of the function
# in the later part of the program, use return statement.

sal = int(input("Enter Salary: "))

def compute():
    tax = sal*10/100
    return tax
t = compute()
netsal = sal-t
print(f"net salary = {netsal}")

#--------------------------------------------------
print("-------------")

def display(s1,s2,s3):
    tot = s1+s2+s3
    avg = tot/3
    return tot,avg

p,q = display(60,70,80)
print("total = ",p)
print("avg = ",q)
#or
x = display(20,26,23)
print(x,type(x))
print("Total = ",x[0])
print("AVG = ", x[1])

#-----------------------------------------------------
print("-------------------")

# Functions without arguments and with return type

def add4():
    x=10
    y=20
    z = x+y
    return z

res = add4()
print(res)






