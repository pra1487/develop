l = [1,2,3,4,-2,-3,4,6,7,-5]

def sum_any_two(x):

    set1 = set(x)
    if len(x)<2:
        print("input list is having only one element")
    else:
        for i in x:
            if -i in set1:
                return True
            else:
                return False

l2 = [3]
l3 = [1,2,3,4]
print(sum_any_two(l3))



