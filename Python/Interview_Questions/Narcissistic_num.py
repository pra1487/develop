x = 1234
import math

def func(x):
    """ check the number is Narcissistic or not"""
    pow = len(str(x))
    sum = 0
    for i in str(x):
        sum = sum+(math.pow(int(i),pow))

    if (x==sum):
        print(f"Yes {x} = {sum}")
    else:
        print(f"No {x} != {sum}")

func(1634)