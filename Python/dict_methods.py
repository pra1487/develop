emp = {"name":"Prasad","ID":101,"sal":30000,"City":"Hyd","Details":["white","apple",3344]}

#.copy()
emp1 = emp.copy()  # Copy and assigning to another variable
print(emp1)

x = ("k1","k2","k3")
y = 123

new_dic = dict.fromkeys(x,y)
print(new_dic)               # creating a dictionary.

#get(key)
print(emp.get("name"))  # get the value of the specified key

print(emp.get("deign","Benz"))  # if the specified key not exists than it will display the default one.

#items()
print(emp.items())   # the key-value pairs of the dictionary, as tuples in a list.

#keys()
print(emp.keys())   # giving the list of keys

#pop()
d = emp.pop("ID")   # removing the pair of the specified key
print(emp)
print(d)

#popitem()
d1 = emp.popitem()  # removing the last element from the dict
print(d1)
print(emp)


car = {
  "brand": "Ford",
  "model": "Mustang",
  "year": 1964
}

# setdefault()
x = car.setdefault("fuel","petrol")   #Returns the value of the specified key. If the key does not exist: insert the key, with the specified value
print(x)

#update
car.update({"fuel":["petrol","desel"]})  # Adding new key value pair to the dict
print(car)

#values
print(car.values())  # get the values as tuple of list


# ==============================
emp = {"name":"virat", "sal":23000}
emp1 = {"des":"batsman", "city":"delhi"}

def dic_mer(x,y):
    res = {**x, **y}
    return res

nd = dic_mer(emp, emp1)
print(nd)


