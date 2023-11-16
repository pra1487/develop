# Local and global variables with same name

x = 10

def show():
    x = 20
    print(x)   # Always local variable gives first preference

def display():
    print(x)

show()
display()
