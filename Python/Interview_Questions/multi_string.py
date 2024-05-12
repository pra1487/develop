s = "abc"

def multi_string(s,n):
    if(len(s)==0):
        return ""
    else:
        return s[0]*n+multi_string(s[1:],n)
print(multi_string(s,5))