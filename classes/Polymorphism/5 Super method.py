# In constructor overriding , we can not access base class NSVs
# In constructor overriding, using derived class object accessing both base and de
# for that the solution is by using super() method.


# super(): This is used to call super class method or constructor through
# sub class method or constructor

class A:
    def __init__(self):
        print("super class constructor")
        self.x = 10
class B(A):
    def __init__(self):
        super().__init__()
        print("from constructir B")
        self.y = 20
        print("x=",self.x)
        print("y=",self.y)


b1 = B()

# Here first super class constructor executes followed by sub class constructor.
# Here both logics got executed.
