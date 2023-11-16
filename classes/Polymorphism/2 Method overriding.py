# Method overriding: The concept of defining a method with same name and same arguments
# in both super class and sub class is known as method overriding
# Here always derved class method is overriding the existing method.

class x:
    def m1(self):
        print("from base class x")

class y(x):
    def m1(self):
        print("from derived class y")

y1 = y()
y1.m1()

# Here Sub class method is called and super class method is overridden.
# to execute super class method, needs to create object on super class.

x1 = x()
x1.m1()

# Note: method overtiding is seen only in inheritance.
