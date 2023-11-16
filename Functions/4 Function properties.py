# Function properties:

# 1. A functon can be called multiple times

def display():
    print("Hello")
    print("Good Morning \n")

display()
display()

#---------------------------------------------

# The order in which the functions are defined and the order in which they are called need not be the same.

def English():
    print("ABCDEFGHI....Z")

def Maths():
    print("1234566....")

def chemistry():
    print("H2O is water")

def physics():
    print("Newtons Third law \n")

chemistry()
Maths()
English()
physics()

#-----------------------------------------------

# A function can call another function

def hyd():
    print("Hello Hyderabad..!")
    mum()
    
def mum():
    print("Hello Mumbai..!")

hyd()

# -------------------------------------------------------

# display1() # Gives error because we can not make a func call before define it. 

def display1():
    print("Good Morning...!")

def display2():
    print("Good Evening...!")

display1()

#-------------------------------------------------------

def compute():
    print("Hello world")
    def show():
        print("Hello India")
    show()  # func call in function
    
compute()
#show() # can not execute.




