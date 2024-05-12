class A:
    a = 10
    b = 20

    def m1(self):
        print("from super class A:")
        self.x = 4.5
        print("a=",A.a)
        print("b=",A.b)
        print("x=",self.x)

class B(A):
    c = 30
    d = 40
    a = 50
    def m2(self):
        print("from derived class B:")
        B.a = B.a+5
        print("a=",B.a,id(B.a))
        print("b=", B.b) # Accessing b of class A
        print("c=",B.c)
        print("d=",B.d)
        self.e = B.a+B.b+B.c+B.d
        print("e=",self.e)
        print("\n\n\n")
        self.m1() # Accessing method of class A
        print("x=",self.x)

b1 = B()
b1.m2()
b1.m1()
print(B.a)
        
