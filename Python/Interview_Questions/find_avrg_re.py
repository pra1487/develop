def avrg():

    n = int(input("Enter the required number of numbers:"))
    l = []

    for i in range(n):
        num = int(input('Enter number here:'))
        l.append(num)
    avrg = sum(l)/n

    return avrg

def avrg1():

    count = int(input('Enter count of numbers:'))
    sum = 0

    for i in range(count):
        num = int(input('enter the number here:'))
        sum += num

    avrg = sum/count
    return avrg

print(avrg1())
