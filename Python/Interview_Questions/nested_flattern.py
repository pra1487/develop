l = [[1,2],['virat', 24], 45,'prasad', ['dhoni', 'pandya']]

def flat_list(x):

    rl = []

    for i in x:
        if(type(i)==list):
            rl.extend(i)
        else:
            rl.append(i)
    return rl

print(flat_list(l))


in_li = [1,2,3,[4,[5,6,[8,9]]]]

def flat_list2(li):
    fl = []

    for i in li:
        if (type(i) is list):
            fl.extend(flat_list2(i))
        else:
            fl.append(i)

    return fl

print(flat_list2(in_li))