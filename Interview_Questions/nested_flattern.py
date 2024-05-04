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