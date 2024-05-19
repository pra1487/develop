li = ['pawan 44', '12 prasad', '24 kumar']


def func(x):
    rl = []
    for i in x:

        rs = ''
        for j in i:
            if j.isdigit() == True:
                rs += j
        rl.append(int(rs))

    return rl


print(func(li))

