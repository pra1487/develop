# Overriding a constructor

class x:
    def __init__(self):
        print("Base class constructor")

class y(x):
    def __init__(self):
        print("Derved class constructor")

y1 = y()

# Here derived class constructor has been executed.
# if you want to execute the super class constructor, create an object on super class

x1 = x()

# Note: Constructor overloading will not work in python.
