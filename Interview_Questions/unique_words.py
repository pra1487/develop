str = 'hello world python is very good language and python is having python very good'

def uw(x):

    ul = []
    for i in x.split():
        if i not in ul:
            ul.append(i)

    uwc = len(ul)
    return uwc

print(uw(str))

""""
import pandas as pd

l = str.split()
wcount = pd.Series(l).value_counts()
print(wcount)

"""

def word_count(x):

    counts = dict()
    words = x.split()

    for i in words:
        if i in counts:
            counts[i]+=1
        else:
            counts[i]=1
    return counts

wcd = word_count(str)
for key in wcd.keys():
    print(key, wcd[key])
