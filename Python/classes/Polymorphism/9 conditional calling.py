x = int(input("Enter value of X: "))

class A:
    def m1(self):
        print("Odd Number")

class B():
    def m1(self):
        print("Even number")

a1 = A()
b1 = B()

if (x%2==0):
    b1.m1()
else:
    a1.m1()
