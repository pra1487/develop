x  = 124

def func(x):
    c = 0

    for i in str(x):
        if (x%int(i)==0):
            c += 1
    if (c == len(str(x))):
        print("Yes")
    else:
        print("No")

func(x)