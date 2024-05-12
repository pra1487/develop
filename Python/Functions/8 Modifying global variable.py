# Modifying the global variable

x = 10   # Global variable

def display():
    global x # forward decleration
    x = 20
    print(x)

def show():
    print(x)

show()   # controle will goes to directly to show() func
display()
show()
