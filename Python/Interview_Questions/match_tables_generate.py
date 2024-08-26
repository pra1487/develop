def run():

    table_num = int(input("Please enter the table number:"))
    table_range = int(input('Please enter the table range:'))

    for i in range(1,table_range+1):
        print(f"{table_num}*{i}={i*table_num}")

run()
