class sample:
    def __init__(self, *x, **y):
        self.x = x
        self.y = y
        print('x=',self.x, type(x))
        print('y=',self.y, type(y))

obj1 = sample(10,20,30, age=30, name='krish')
