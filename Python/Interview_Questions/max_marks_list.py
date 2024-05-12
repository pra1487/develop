st = [('virat',60),('dhoni',100),('surya',65),('sachin',70)]

def func(li):

    if (len(li)==0):
        return []
    else:
        max_marks = max(marks for name, marks in li)
        result = [(name, marks) for name, marks in li if marks==max_marks]
        return result

print(func(st))