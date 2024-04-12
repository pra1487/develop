li = [1,-1,-2,-3,3,4,5,6,-8,-7,-9]

def pos_cnt(x):

    pc = 0
    nc = 0

    for i in x:
        if i>0:
            pc = pc+1
        else:
            nc = nc+1
    return pc, nc

x, y = pos_cnt(li)
print(f"positive numbers count is: {x}")
