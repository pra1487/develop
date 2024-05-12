    x = "Hellow world This Python IS VERY gooD For CAREEr"

def count(x):

    up_cnt = 0
    lw_cnt = 0

    for i in x:
        if i.isupper()==True:
            up_cnt += 1
        elif (i.islower()==True):
            lw_cnt += 1

    return up_cnt, lw_cnt

x,y = count(x)
print(f"Upper count is: {x}")
