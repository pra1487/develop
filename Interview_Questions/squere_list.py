li = list(range(1,20))

def squere(x):

    sql = []

    for i in x:
        sql.append(i**2)

    return sql
print(squere(li))
