in_str = 'lakshmiprasad1487@gmail.com'

def hide_email(x):

    email = x.split('@')[0]
    domain = x.split("@")[1]

    result = email[0]+(len(email)-2)*'*'+email[-1]+"@"+domain

    return result

print(hide_email(in_str))
