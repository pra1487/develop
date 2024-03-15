def find_fact():

    num = int(input("Enter number here:"))
    fact = 1

    if (num==0):
        print('Factorial of zero is 1')
    elif(num<0):
        print('Please enter positive numbers')
    else:
        for i in range(1,num+1):
            fact = fact*i
        print(f"Factorial of the {num} is: {fact}")

find_fact()