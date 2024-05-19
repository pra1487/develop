s = "This is prasad"

def rw(x):
    wl = x.split()
    wl.reverse()

    result = " ".join(wl)
    return result

print(rw(s))