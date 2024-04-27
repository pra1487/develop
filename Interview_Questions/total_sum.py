li = [1,2,3,4,5,6,7,8,9]

def sum_d(x):

    sum_d = 0
    for i in x:
        sum_d = sum_d+i

    return sum_d

x = sum_d(li)
print(x)


def even_sum(x):
    even_s = 0

    for i in x:
        if i%2==0:
            even_s = even_s+i
    return even_s

print(even_sum(li))

