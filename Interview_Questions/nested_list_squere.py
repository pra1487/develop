l = [[1,2,3],[4,5,6],[7,8,9]]

import math

def sq_li(x):
    res_li = []

    for i in x:
        int_li = []
        for j in i:
            int_li.append(int(math.pow(j,2)))

        res_li.append(int_li)

    return res_li

print(sq_li(l))

# list comprehensing method.

res = [[int(math.pow(j,2)) for j in i] for i in l]
print(res)