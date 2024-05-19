# Accessing NSV of super class constructor from sub class constructor using super()

class A:
    def __init__(self):
        self.x = 10
        print("from super class A")

class B(A):
    def __init__(self):
        super().__init__()
        self.y = 20
        print("from sub class B")

b1 = B()
print(b1.x) # accessing super class NSV 'x' with sub class object
print(b1.y)
