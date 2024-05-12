li = ['pawan 44', '12 prasad', '24 kumar']

def find_int(l):
    rsl = []
    for i in l:
        l1 = i.split()
        for j in l1:
            if j in '012443':
                rsl.append(int(j))
    return rsl

print(find_int(li))
