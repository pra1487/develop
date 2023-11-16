# Sub class method calling super class method

class A:
    def m1(self):
        self.x = 10
        print(f"x = {self.x}")

class B(A):
    def m1(self):
        super().m1()
        self.y = 20
        print(f"y= {self.y}")

b1 = B()
b1.m1()
