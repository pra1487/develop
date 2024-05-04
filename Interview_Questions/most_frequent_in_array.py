a = [1,1,2,1,4,2,3,4,5,2,2]

def array_frequent(li, k):

    """ finding the most frequent elements in array"""

    counts = dict()
    result = set()
    for i in li:
        if (i in counts):
            counts[i]+=1
            if (counts[i]>2):
                result.add(i)
        else:
            counts[i]=1
    return result

print(array_frequent(a,2))