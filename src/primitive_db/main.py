#!/usr/bin/env python3


from .engine import run


def main():
    print("***База данных***\n")

    print("Функции (управление таблицами):")
    print("<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print()

    print("***Операции с данными***")
    print(
        "<command> insert into <имя_таблицы> values (<значение1>, <значение2>, ...)"
        " - создать запись."
    )
    print("<command> select from <имя_таблицы> - прочитать все записи.")
    print(
        "<command> select from <имя_таблицы> where <столбец> = <значение>"
        " - прочитать записи по условию."
    )
    print(
        "<command> update <имя_таблицы> set <столбец> = <значение>"
        " where <столбец_условия> = <значение_условия>"
        " - обновить запись."
    )
    print(
        "<command> delete from <имя_таблицы> where <столбец> = <значение>"
        " - удалить запись."
    )
    print("<command> info <имя_таблицы> - вывести информацию о таблице.")
    print()

    print("Общие команды:")
    print("<command> help - справочная информация")
    print("<command> exit - выход из программы\n")

    run()




if __name__ == "__main__":
    main()