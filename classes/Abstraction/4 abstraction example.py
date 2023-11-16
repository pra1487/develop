class sample:
    hname = input("Enter husband name: ")
    wname = input("Enter wife name: ")
    hage = float(input("Enter husband age: "))
    __wage = float(input("Enter wife age: "))
    __sal = float(input("Enter salary"))

s1 = sample()
print("Hname=", sample.hname)
print("wname=", sample.wname)
print("Hage=", sample.hage)
#print("wage=",sample.__wage)
#print("sal = ", sample.__sal)

class B:
    print("husband age = ", sample.hage)
    #print("Salary = ", sample.__sal)
    
