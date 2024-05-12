def sum_digit():

    for i in range(100,110):
        num = i
        sum = 0

        while num!=0:
            digit = num%10
            sum += digit
            num = num//10
        if sum%2 == 0:
            print(i)
sum_digit()