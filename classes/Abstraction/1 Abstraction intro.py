# Abstraction: Hiding data and methods
# The concept of hiding the properties(data + Methods) of one class from other class
# or outside class is known as Abstraction.
# ex: SV can be accessed from outside the class using class name, but i want to restrict
# in order to hide the properties of a class, we use __ at the beginning of the property.
# ex: __x = 10

# hiding a static variable

class sample:
    __x = 10  # now x is hidden and can be accessed with in the class only
    y = 20
    print(__x)
    print(y)

    def display(self):
        print(sample.__x, type(sample.__x))
        print(sample.y, type(sample.y))
# end of the class

s1 = sample()
s1.display()
print(sample.y)
#print(sample.__x)  # we can not able to access from out side the class

class test:
    a = 100
    def show(self):
        print('a=',test.a)
        print('x=',sample.__x)  # we can not able to access in other class as well
        print('y=',sample.y)
t1 = test()
t1.show()





