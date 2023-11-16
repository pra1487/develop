# boolean():    A function which returns True or False

x = int(input("Enter x value: "))
y = int(input("Enter y value: "))

def display():
    if(x>y):
        return True
    else:
        return False
res = display()
print(res)
