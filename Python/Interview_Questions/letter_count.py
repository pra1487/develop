s = 'abbcccdddd'


def letter_count(x):
    count = 1
    l = []

    for i in range(1, len(x)):
        if (x[i] == x[i - 1]):
            count += 1
        else:
            l.append(count)
            count = 1
    l.append(count)

    return l


print(letter_count(s))
