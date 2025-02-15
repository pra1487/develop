import random
import string

def generate_password(length):
    password = "".join(random.choices(string.ascii_letters+string.digits, k=length))
    return password

print(generate_password(8))
