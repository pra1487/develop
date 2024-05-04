x = 1234
import math

def func(x):
    pow = len(str(x))
    sum = 0
    for i in str(x):
        sum = sum+(math.pow(int(i),pow))

    result = sum
    return sum

print(func(x))