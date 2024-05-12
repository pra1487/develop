str1 = "Prasad Is Very Good."

print(str1.upper())  # convert to upper
print(str1.title())  # ever first letter of every word will covert to upper
print(str1.count('a')) # give the number of accurances in string
print(str1.split(" ")) # splitting and giving the list
print(len(str1))  # give the number of characters in string
print(str1.capitalize()) # convert the first letter to upper

print(str1.casefold()) # convert to lowercase


str2 = "p\tra\ts\tad"

print(str2.expandtabs(2))  # creating tabs at \t
print(str1.endswith("."))  # return True or False
print(str1.startswith("I"))

print(str1.find('iS'))  # give the index number for the mentioned, if not found give -1
print(str1.index(' ')) # Searches the string for a specified value and returns the position of where it was found

str3 = "123 345"
print(str3.isalnum()) # give True or False
print(str3.isalpha()) # give True or False
print(str1.isascii()) # will check the ASCII and return True or False

print(str3.isdecimal()) # True or False
print(str3.isidentifier())
print(str3.islower())
print(str3.isnumeric())
print(str1.isprintable())
print(str1.isspace())

print(str1.istitle())
print(str1.isupper())

print("#".join(str1))


txt1 = 'apple'
print(txt1.ljust(12), 'is my favorate fruit')

txt2 = '    Apple   '
print(txt2.lstrip(), "is my favorate")
print("Of all fruits",txt2.strip(),"is my favourate")

y = str.maketrans('a','A')
print(txt1.translate(y))

print(txt1.replace("bananas","Apples"))

txt = "I could eat \nbananas all day I"
print(txt.partition('bananas'))

print(txt.rfind("I"))
print(txt.rindex("I"))

print(txt.split()) # gives list
print(txt.splitlines()) # give list

print(txt.swapcase())

txt3 = 'num'
print(txt.zfill(100))

