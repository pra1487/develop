"""
Inheritance:
============

Inheritance: The Concept of making availble the properties of one class
                to another class is called inheritance.

    * Inheritance is nothing but deriving a class from another existing class.
    * The derived class will have the combined features of both the existing and new class.

    * The existing class is called as super class or base class or parent class.
    * The new class is called as sub class or derived class or child class.

    * Base class is a class which new class can be created.
    * Child class is a class that inherits or extends the features of base class.

    (or)

    * A class which is extended by another class is known as super/parent class.
    * A Class which is extending another class is known as sub/child/derived class.

 """
#ex:

class A:
    a = 10
    b = 20
    def m1(self):   # Class B Extending class A (Inheritance)
        pass

class B(A):
    c = 30
    d = 40
    def m2(self):
        pass

# There are 2 variables (a,b) and 1 method (m1) in class A.
# There are 4 variables (a,b,c,d) and 2 methods (m1,m2) in class B
# Here class B is extending class A, means class B having combined features of both the classes
# class B(sub class) can access all the properties of class A
