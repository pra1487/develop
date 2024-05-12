li = ['01-02-2024', '02-02-2024', '03-03-2024']

def func(li):

    dd = {"01":'Jan',"02":'Feb',"03":'Mar'}
    rl = []

    for i in li:
        month_number = i.split('-')
        month_name = dd[month_number[1]]
        rl.append(month_number[0]+'-'+month_name+'-'+month_number[1])
    return rl

print(func(li))