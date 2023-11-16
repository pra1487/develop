"""
Arbitrary Args or Variable length args (*args,**kwargs):
    Here during fn call, we can specify more number of parameters vaues than
    specified in the function definition.

    An asterisk(*) is placed at before the variable name that holds multiple values.
    which is in tuple form

"""

def display(name,*marks):
    print(name,type(name))
    print(marks,type(marks))
    for p in marks:
        print(p)

display("Prasad",75,65,85)
print("\n\n")

#--------------------

def display1(name, *scores):
    print(name)
    x = 1
    for i in scores:
        print(f"match {x}: {i}")
        x+=1
display1("Virat",102,87,82)

#-------------------------------



