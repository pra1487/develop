emp_list = [('prasad',10000),('Rahul',12000),('mahesh',14000),('pavan',11000),('Mohit',13000),('Dheeraj',21000)]

def averg_salary(emp_list):

    '''Finding emps who are having more than average salary of the list of emps'''

    total_salary = 0
    flist = []
    for name, salary in emp_list:
        total_salary = total_salary+salary

    avr_salary = total_salary/len(emp_list)

    for name, salary in emp_list:
        if (salary > avr_salary):
            flist.append((name,salary))

    return flist

result = averg_salary(emp_list)
print(result)

