class sample:
    a = 4.5  # static variables
    b = 2.6

    def display(self):
        self.x = 10  # nsv
        self.y = 20
        print('x=', self.x)
        print('y=', self.y)
        print('a=', sample.a)
        print('b=', sample.b)
        # modifying the values
        print('After modify')
        self.x = self.x + 50
        self.y = self.y + 60
        sample.a = sample.a + 70
        sample.b = sample.b + 80
        print('x=', self.x)
        print('y=', self.y)
        print('a=', sample.a)
        print('b=', sample.b)


obj1 = sample()
obj1.display()

