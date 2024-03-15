def anagr_chk():

    s1 = input("Enter string1:")
    s2 = input('Enter string2:')

    if (sorted(s1) == sorted(s2)):
        print(f'The entered strings {s1}, {s2} are anagrams')
    else:
        print(f"{s1} and {s2} are not anagram strings")

anagr_chk()