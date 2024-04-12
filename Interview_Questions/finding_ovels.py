s = "Process finished with exit code 0"

def vol_find(x):

    vol_l = []

    for i in x:
        if i in 'aeiouAEIOU' and i not in vol_l:
            vol_l.append(i)

    vol_l.sort()

    return vol_l

print(vol_find(s))