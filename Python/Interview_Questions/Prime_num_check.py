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

def run():

    num = int(input("Please enter number:"))
    flag = False

    if num==0:
        return "Please enter the number otherthan 0"
    elif(num<0):
        return "Please enter positive number"
    else:
        for i in range(2,num):
            if(num%i==0):
                flag = True
                break
            else:
                flag
    if flag == True:
        return "No not a prime"
    else:
        return "Yes Prime"

print(run())

