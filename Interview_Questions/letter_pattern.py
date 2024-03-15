d = 'abbcccddddeeeee'

previous = d[0]
output = ''
c = 1
i = 1

while (i<len(d)):
    if (previous == d[i]):
        c = c+1
    else:
        output = output+str(c)+previous
        previous = d[i]
        c = 1
    if (i==len(d)-1):
        output = output+str(c)+previous
    i += 1

print(output)