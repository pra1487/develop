li1 = [1,3,2,4,6,5]
li2 = [1,2,3,7,6,8]
li3 = [1,2,3,4,5,6]

def li_sort_check(li):

    if li == sorted(li):
        print(f'{li} is sorted')
    else:
        print(f"{li} is not sorted")

li_sort_check(li1)
li_sort_check(li2)
li_sort_check(li3)
