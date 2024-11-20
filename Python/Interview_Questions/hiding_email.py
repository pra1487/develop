in_str = 'lakshmiprasad1487@gmail.com'

def hide_email(x):

    email = x.split('@')[0]
    domain = x.split("@")[1]

    result = email[0]+(len(email)-2)*'*'+email[-1]+"@"+domain

    return result

print(hide_email(in_str))


in_str1 = ['lakshmiprasad1487@gmail.com', 'pessu1487@gmail.com']

def run(s):
    rl = []
    for i in s:
        name = i.split('@')[0]
        domain = i.split('@')[1]
        new_mail = name[0] + '*' * (len(name) - 2) + name[-1] + '@' + domain
        rl.append(new_mail)
    return rl


print(run(in_str))


