class customer:
    '''customer application'''
    Bank_name = 'SBI'

    def get_details(self):
        self.cname = input('customer name pls:')
        self.accno = int(input('Enter account number:'))
        self.address = input('Enter Address:')
        self.balance = int(input('Enter balance:'))
    def put_details(self):
        print(f"Cusstomer Name: {self.cname}")
        print(f"Customer Account No: {self.accno}")
        print(f"Customer Address: {self.address}")
        print(f"Customer Account Balance: {self.balance}")
    def deposit(self):
        self.damount = int(input('Enter Deposit amount:'))
        self.balance = self.balance+self.damount
        return f"Total balance after deposit of '{self.damount}' is: {self.balance}"
    def withdraw(self):
        self.wdramount = int(input('Enter withdraw amount:'))
        self.balance = self.balance-self.wdramount
        return f"Total balace after withdrawls of '{self.wdramount}' is: {self.balance}"

c1 = customer()
c1.get_details()
c1.put_details()
print(c1.deposit())
print(c1.withdraw())