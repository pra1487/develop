def compute():
    """Functions to compute simple interest"""
    
    p = int(input("enter the principle amount: "))
    t = float(input("enter the time details: "))
    r = float(input("enter the rate of interest value: "))

    si = (p*t*r)/100
    print(f"Simple ineterest is: {si} ")

compute()

""" End of the function """
