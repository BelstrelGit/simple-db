import shlex

from .core import create_table, drop_table, list_tables
from .utils import load_metadata, save_metadata

META_FILE = "db_meta.json"


def print_help():
    """Prints the help message for the current mode."""
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу") # noqa: E501
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")


def run():
    while True:
        metadata = load_metadata(META_FILE)
        user_input = input("Введите команду: ").strip()
        if not user_input:
            continue

        try:
            args = shlex.split(user_input)
        except ValueError:
            print(f"Некорректное значение: {user_input}. Попробуйте снова.")
            continue

        match args:
            case ["help"]:
                print_help()

            case ["exit"]:
                break

            case ["list_tables"]:
                list_tables(metadata)

            case ["create_table"]:
                print("Некорректное значение: отсутствует имя таблицы. Попробуйте снова.") # noqa: E501

            case ["create_table", table, *cols]:
                metadata = create_table(metadata, table, cols)
                save_metadata(META_FILE, metadata)

            case ["drop_table"]:
                print("Некорректное значение: отсутствует имя таблицы. Попробуйте снова.") # noqa: E501

            case ["drop_table", table]:
                metadata = drop_table(metadata, table)
                save_metadata(META_FILE, metadata)

            case [cmd, *_]:
                print(f"Функции {cmd} нет. Попробуйте снова.")
