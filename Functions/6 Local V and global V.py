# Local and global variables:

# Local Variable: The variables which are defined within the functions and can be accessed only within the function.

# Global Variables: The variables which are defined outside the function and can be accessed by all the functions.

x = 10  # Global variable

def display():
    y = 20  # local variable
    print(x)
    print(y)

def show():
    print(x)
    #print(y) # can not accessable the local variable of another func

display()
show()
