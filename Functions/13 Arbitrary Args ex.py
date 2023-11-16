def display(*p,q,r=20):
    print(p,type(p))
    print(q,type(q))
    print(r,type(r))

display(10,20,q=30)
print("\n")
display(10,20,q=40,r=50)
print("\n")
display((10,20,30),(40,50),q=40,r=50)
