class x:
    def __init__(self):
        print("Base class constructor...")
    def m1(self):
        print("from m1...class X")

class y(x):
    def m2(self):
        print("from m2 class y")

y1 = y()
y1.m1()
y1.m2()
print("\n\n")

print(x.__name__)
print(x.__bases__)
print(y.__bases__)

# If we have constructor in super class and sub class and if we create object
# of sub class, then sub class constructor only will execute.
# If we execute super class constructor also, create object on super class.
