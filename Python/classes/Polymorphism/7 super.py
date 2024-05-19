
class A1:
    def __init__(self):
        print("from base class A1 cons")
class A2:
    def __init__(self):
        print("from class A2 cons")

class B(A1, A2):
    def __init__(self):
        super().__init__()
        print("from cons B")

b1 = B()
