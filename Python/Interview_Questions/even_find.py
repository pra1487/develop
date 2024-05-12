lis = [1,2,3,4,5,7,6,8,9,10]

def even_find(x):
    el = []

    for i in x:
        if ((i%2==0) and (i not in el)):
            el.append(i)
    return el

x = even_find(lis)
print(x)
