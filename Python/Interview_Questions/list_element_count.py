li = ['prasad','siva','siva','virat','prasad','jaddu','prasad']

def li_ele_cnt(l):

    counts = dict()

    for i in l:
        if (i in counts):
            counts[i]+=1
        else:
            counts[i]=1
    return counts

x = li_ele_cnt(li)

for i in x.items():
    print(i[0],i[1])
