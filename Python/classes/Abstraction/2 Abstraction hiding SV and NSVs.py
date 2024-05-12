# hiding static and NSV

class sample:
    __x = 100  # hiding SV
    def display(self):
        self.__y = 200       # hiding NSV
        print(sample.__x)
        print(self.__y)

s1 = sample()
s1.display()

#print(sample.__x)   # we can not able to access from out side of the class
#print(s1.__y) # we can not access from out side of the class.


