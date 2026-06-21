building_view = [4, 2, 3, 1]

def run(li):
    """ Fetch the index positions of building which can visible from right side"""

    max_high = 0
    rl = []

    for i in range(len(li)-1,-1,-1):
        if(li[i]>max_high):
            max_high = li[i]
            rl.append(i)
        else:
            pass
    return rl[::-1]

print(run(building_view))