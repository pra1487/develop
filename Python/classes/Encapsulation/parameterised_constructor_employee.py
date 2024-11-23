class employe:
    """Employee Application"""
    company = 'Cognizant'
    empcount = 0

    def __init__(self, empname, empid, empsal, design='SE', *languages, **details):
        self.empname = empname
        self.empid = empid
        self.empsal = empsal
        self.design = design
        self.languages = languages
        self.details = details
        employe.empcount = employe.empcount+1
    def display(self):
        print('Name=',self.empname,'\nID=',self.empid,'\nSalary=',self.empsal,'\nLangs=',self.languages,'\nDetails=',self.details,)

emp1 = employe('Virat',101,12000,'SA','ENG','HINDI', age=37, city='BANG')
emp1.display()