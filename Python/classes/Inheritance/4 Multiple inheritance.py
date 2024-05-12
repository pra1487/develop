# Multiple inheritance:

class A:
    x=10
    y=20
    def m1(self):
        print("from class A")
        print("x=",A.x)
        print("y=",A.y)
class B:
    a=3.5
    b=4.7
    def m1(self):             # same methods in class A and B
        print("from class B")
        print("a=",B.a)
        print("b=",B.b)
class C(A,B):
    def m2(self):
        intsum = C.x+C.y
        floatsum = C.a+C.b
        print("Integer sum = ",intsum)
        print("Floats sum = ",floatsum)

c1 = C()
c1.m1()
c1.m2()
    
