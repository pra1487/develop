class customer:
    bank = 'SBI'

    def __init__(self, name, acno, addr, bal):
        self.name = name
        self.acno = acno
        self.addr = addr
        self.bal = bal
    def depo(self, depamnt):
        self.bal = self.bal+depamnt
        return self.bal
    def wthdr(self, wtdramnt):
        self.bal = self.bal-wtdramnt
    def balenq(self):
        print(f"balance: {self.bal}")
c1 = customer('prasad',123,'KPHB', 12000)

print(c1.depo(2000))
