s = "Process finished with exit code 0"

def vol_find(x):

    vol_l = []

    for i in x:
        if i in 'aeiouAEIOU':
            vol_l.append(i)
    return vol_l

print(vol_find(s))