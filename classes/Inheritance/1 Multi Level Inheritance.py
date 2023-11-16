# Milti-Level Inheritance: Deriving a class from another derived class.

m = 45

class A:
    x = 10
    y = 20
    print("m=", m)
    def m1(self):
        global m  # forward declaration
        m = 55
        print("m=",m)
        print("x=",A.x)
        print("y=",A.y)

class B(A):
    z=30
    def m2(self):
        global m
        m=60
        total = B.x+B.y+B.z
        print("z=",B.z)
        print("total=",total)

class C(B):
    p = 70
    def m3(self):
        self.r = 80
        C_total = C.z+C.p+C.x
        print("c_total = ",C_total)

c1 = C()
c1.m1()
c1.m2()
c1.m3()

print(C.__bases__)
print(B.__bases__)
print(A.__bases__)
        
