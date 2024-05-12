# Program illustrating inheritance
# program with global, local, SV and NSV
# program with a function and a method

p = 70 # GV

def show1():  #function
    print("hello")

class A:
    x = 10
    print(x)
    def __init__(self):
        self.y = 20
        print("constructor of class A")
    def display(self):
        print("Hello...from class A")
        print("p=", p)
        show1()

class B(A):
    z = 30
    print(z)
    def display2(Self):
        print("Hello... from class B")
    def show(self):
        global P  # forward declaration
        print("x=",B.x)
        print("y=",self.y)
        print("z=",B.z)
        p = 75
        sum1 = B.x+self.y+B.z
        print("sum=",sum1)
        print("p=",p)
        self.display()
        self.display2()
        show1()

b1 = B()
b1.show()

print(B.x)
print(b1.y)
