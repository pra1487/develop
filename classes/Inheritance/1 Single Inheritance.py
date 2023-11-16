# Single Inheritance: A Derived class with a single base class.

class A:
    def m1(self):
        print("Module m1 from class A")

a1 = A()
a1.m1()
print(A.__bases__)

# Object class is the base class for all the classes in python.
# Here A is a derived class with a single base class (object class)
# If a class is not extending any class then by default object class is the base class.
