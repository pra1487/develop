# Different Types of function arguments.
"""
1. Non-default or Positional Arguments
2. Default
3. Keyword
4. Non-Keyword
5. Arbitrary or variable length

"""

# 1. Non-Default Arguments: Arguments specified in the function definition and Argument values compulsorily we need to specify with in the function call.

def display(x,y):
    z = x+y
    print(z)

display(12,23)


#2. Default: Arguments specified in the function definition with some default values.
#           and it is not compulsory to specify in the function call.

def display1(x=10, y=20):
    z = x+y
    print(z)

display1()      # execute with default values
display1(50)    # Execute with default and specified
display1(40,40)  # execute with specified values.

# Non default and default combo

def display2(x,y=20):  # non-def and def args
    z = x+y
    print(z)

display2(10)
display2(60,50)

# Non-default and default args are allowed but default and non-def not allowed.
# default args can not come first






