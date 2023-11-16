class sample:
    __x = 10
    def __display(self):
        self.__y = 20
    def show(self):
        self.__display()
        print("x=",sample.__x)
        print("y=",self.__y)

s1 = sample()
#s1.__display()
s1.show()
#print(sample.__x)
#print(s1.__y)
