li = ['dhoni200', 'pandya40','jaddu50','virat10','rahul30']

def sort_li(x):

    rl = []
    for i in x:
        c = ""
        for j in i:
            if(j.isdigit()):
                c += j
        rl.append([int(c),i])
    rl.sort(key=lambda x:x[0])

    return [i[1] for i in rl]
print(sort_li(li))