s = "hello prasad how are you prasad how about you"

def word_count(x):

    wl = x.split()
    counts = dict()

    for i in wl:
        if i in counts:
            counts[i]+=1
        else:
            counts[i]=1
    return counts

x = word_count(s)
for i in x.items():
    print(i[0], i[1])
