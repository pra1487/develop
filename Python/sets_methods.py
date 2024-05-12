s = {1,2,3,4,"hij","klm"}

#copy()
s1 = s.copy()
print(s1,type(s1))   # copying and assign to another variable

#len()
print(len(s))  # give number of elements in set

#add()
s.add("copper")  # adding new element
print(s)

#difference()
print(s.difference(s1))  # will give non common elements

#intersection()
print(s.intersection(s1))   # will give common elements

#issubset()
print(s1.issubset(s1))   # it will check weather the set is available in another set and will give boolean values True or False

#issuperset()
print(s.issuperset(s1))


#pop()     # will remove random element from the set
s.pop()
print(s)


# remove()    # removes the specific mentioned element.
#s.remove("klm")
print(s)

# discard()   # this is also same as remove() but only difference is remove() will thrown an error if that mentioned
            # element is not in that set but discard() not.

x = {"apple", "banana", "cherry"}
y = {"google", "microsoft", "apple"}

# update()

x.update(y)    # merge two sets but here x will update with y elements.
print("y=",y)
print("x=",x)


# difference_update()

x.difference_update(y)   # removes the items that exist in both sets.
print(x)

# symmetric_difference()   # returns a set that contains all items from both set, but not the items that are present in both sets.

z = x.symmetric_difference(y)
print(z)


# symmetric_difference_update()   # returns original set by removing items that are present in both sets, and inserting the other items.

x.symmetric_difference_update(y)
print(x)
