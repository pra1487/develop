fruits = ['apple', 'banana', 'cherry']

fruits.append('oranges')   # Add new element at end of the list
print(fruits)

fruits.insert(1, 'oranges')  # insert an elemet with index position
print(fruits)

new_f = fruits.copy()   # copy and assign it to another variable
print(new_f)

print(new_f.count('oranges')) # count of number of occurances

fruits.extend(['regi','jama'])
#or
cars = ['bmw','maruti','benz']  # extend, adding list of elements to the current list
fruits.extend(cars)
print(fruits)

print(fruits.index('bmw'))  # index is giving the position in list

fruits.pop(0)   # removes an element with specified position
print(fruits)

fruits.remove('oranges')  # Remove the first specified element
print(fruits)


fruits.reverse()  # reverse the position of the list elements
print(fruits)

fruits.sort()  # sorting in ascending by default
print(fruits)

fruits.clear()  # Remove all the elements from the list
print(fruits)
