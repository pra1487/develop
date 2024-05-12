def prm_check():

    num = int(input("Enter number here:"))
    flag = False

    if num>1:
        for i in range(2,num):
            if num%i == 0:
                flag = True
                break
    if flag:
        print(f'{num} is not a prime number')
    else:
        print(f"{num} is a prime number")

prm_check()