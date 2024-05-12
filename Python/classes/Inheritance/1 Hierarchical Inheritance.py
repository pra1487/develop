# Hierarchical Inheritance: A base class with multiple derived classes.

class Arith:
    x=10
    y = 20
    def m1(self):
        print("x=",Arith.x)
        print("y=",Arith.y)

class Add(Arith):
    def m2(self):
        sum = Add.x+Add.y
        print("sum=",sum)

class Diff(Arith):
    def m3(self):
        diff = Diff.y-Diff.x
        print(f"diff = {diff}")

class Mul(Arith):
    def m4(self):
        mul = Mul.x*Mul.y
        print(f"Mul = {mul}")

class Div(Arith):
    def m5(self):
        div = Div.x/Div.y
        print(f"Div = {div}")

a1 = Add()
a1.m1()
a1.m2()
print("===============")
s1 = Diff()
s1.m1()
s1.m3()
print("=============")

m1 = Mul()
m1.m1()
m1.m4()
print("\n ========")

d1 = Div()
d1.m1()
d1.m5()
print("\n ========")

print(Add.__bases__)
print(Diff.__bases__)
print(Mul.__bases__)
print(Div.__bases__)
print(Arith.__bases__)
