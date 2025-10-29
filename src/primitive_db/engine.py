


import prompt


def welcome():
    print("***")
    print("<command> exit - выйти из программы")
    print("<command> help - справочная информация")

    cmd = prompt.string('Введите команду: ')

    if cmd == "help":
        print("<command> exit - выйти из программы")
        print("<command> help - справочная информация")
