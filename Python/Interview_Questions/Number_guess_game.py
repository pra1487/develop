def number_guess():
    import random

    lower, upper = 1, 100
    secret_number = random.randint(lower, upper)
    attempts, max_attempts = 0,7

    print(f"Guess a number between {lower} and {upper} in {max_attempts}")

    while attempts < max_attempts:
        user_input = input("enter your guess:")

        if not user_input.isdigit():
            print("Invalid input.")
            continue

        guess = int(user_input)

        attempts += 1

        if guess == secret_number:
            print(f"Correct !.. Congratulations!!!!")
            break
        elif(guess < secret_number):
            print('Too low! Try again')
        elif(guess > secret_number):
            print('Too high, Try again')
    else:
        print('Game over')

number_guess()
