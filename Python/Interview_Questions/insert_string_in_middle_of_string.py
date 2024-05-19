s = "City:Kolkata-Westbengal"

result = s.replace("-","-State:")
print(result)

li = [1,2,3,4,5,6,8,4,1,6,10]


def func(li):
    """Printing even numbers which are before odd numbers"""
    rl = []

    for i in range(len(li)-1):
        if(li[i]%2==0 and li[i+1]%2==1):
            rl.append(li[i])
    return rl
print(func(li))