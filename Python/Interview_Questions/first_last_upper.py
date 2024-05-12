s = 'first last avg count sum sum_distinct count_distinct approx_count_distinct'

def conv(x):

    li = x.split()
    res_li = []

    for i in li:
        rw = i[0].upper()+i[1:-1]+i[-1].upper()
        res_li.append(rw)

    result = " ".join(res_li)

    return result

print(conv(s))