class cube:
    def __init__(self):
        self.hight = int(input('Enter the hight:'))
        self.length = int(input('Enter the length:'))
        self.breadth = int(input('Enter the breadth:'))
    def volume(self):
        self.volume = self.hight*self.length*self.breadth
        print(f"Volume of entered cube is: {self.volume}")
cube1 = cube()
cube1.volume()