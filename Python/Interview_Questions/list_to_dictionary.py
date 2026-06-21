li = ['name:virat', 'null:cricket', 'play:batting',"details:['city':'delhi']"]
out_come = {'name':'virat', '-1':'cricket', 'play':'batting',"details":['city','delhi']}

def run(li):

    rl = []

    for i in li:
        if(i.split(':')[0]=='null' or i.split(':')[1]=='null'):
            j = i.replace('null','-1')
            rl.append(j)
        else:
            rl.append(i)
    rd = {}
    for i in rl:
        rd.update({i.split(':')[0]:i.split(':')[1]})
    return rd

print(run(li))