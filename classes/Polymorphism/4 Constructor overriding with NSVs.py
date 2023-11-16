class A:
    def __init__(self):
        self.x = 10
        print("From Base class")

class B(A):
    def __init__(self):
        self.y = 20
        print("from derived class")

a1 = A()
print(a1.x)

b1 = B()
print(b1.y)
#print(b1.x)  #invalid because x is initialised by the main class constructor only


