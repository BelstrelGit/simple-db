# src/primitive_db/engine.py
import csv
import shlex

from prettytable import PrettyTable

from .core import (
    create_table,
    delete,
    drop_table,
    insert,
    list_tables,
    select,
    update,
)
from .parser import parse_set, parse_where
from .utils import (
    load_metadata,
    load_table_data,
    save_metadata,
    save_table_data,
)

META_FILE = "db_meta.json"


def print_help():
    """Prints the help message for the current mode."""
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print()

    print("***Операции с данными***")
    print(
        "<command> insert into <имя_таблицы> values (<v1>, <v2>, ...)"
        " - создать запись"
    )
    print("<command> select from <имя_таблицы> - прочитать все записи")
    print(
        "<command> select from <имя_таблицы> where <col> = <value>"
        " - прочитать записи по условию"
    )
    print(
        "<command> update <имя_таблицы> set <col> = <value>"
        " where <col> = <value> - обновить записи"
    )
    print(
        "<command> delete from <имя_таблицы> where <col> = <value>"
        " - удалить записи"
    )
    print("<command> info <имя_таблицы> - информация о таблице")
    print()

    print("Общие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")



def _headers_from_metadata(metadata, table):
    headers = []
    if table in metadata:
        cols = metadata[table]
        i = 0
        while i < len(cols):
            entry = cols[i]
            if ":" in entry:
                parts = entry.split(":", 1)
                headers.append(parts[0])
            i += 1
    return headers


def _render_table(rows, headers):
    if not rows:
        print("Записей не найдено.")
        return
    t = PrettyTable()
    t.field_names = headers
    i = 0
    while i < len(rows):
        row = rows[i]
        values = []
        j = 0
        while j < len(headers):
            h = headers[j]
            values.append(row.get(h, ""))
            j += 1
        t.add_row(values)
        i += 1
    print(t)


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
                print("Некорректное значение: отсутствует имя таблицы. Попробуйте снова.")  # noqa: E501

            case ["create_table", table, *cols]:
                metadata = create_table(metadata, table, cols)
                save_metadata(META_FILE, metadata)

            case ["drop_table"]:
                print("Некорректное значение: отсутствует имя таблицы. Попробуйте снова.")  # noqa: E501

            case ["drop_table", table]:
                metadata = drop_table(metadata, table)
                save_metadata(META_FILE, metadata)

            # ----------------- CRUD -----------------

            # INSERT: insert into <table> values (<v1>, <v2>, ...)
            case ["insert", "into", table, "values", *value_tokens]:
                tail = " ".join(value_tokens).strip()
                if not (tail.startswith("(") and tail.endswith(")")):
                    print("Некорректное значение: синтаксис insert. Ожидается: insert into <table> values (...).")  # noqa: E501
                    continue
                inner = tail[1:-1]  # содержимое между скобками
                try:
                    # csv.reader корректно разбирает значения с кавычками и запятыми
                    values = next(csv.reader([inner], skipinitialspace=True))
                except Exception:
                    print("Некорректное значение: список значений в insert. Попробуйте снова.") # noqa: E501
                    continue
                print("[DEBUG] raw values:", values)
                # санитизация — уберём случайные пустые элементы и пробелы
                values = [v.strip() for v in values if v is not None and v.strip() != ""] # noqa: E501

                updated = insert(metadata, table, values)
                if updated is not None:
                    save_table_data(table, updated)
                    if len(updated) > 0 and "ID" in updated[-1]:
                        print(f'Запись с ID={updated[-1]["ID"]} успешно добавлена в таблицу "{table}".') # noqa: E501
                    else:
                        print(f'Запись успешно добавлена в таблицу "{table}".')

            # SELECT с WHERE: берём условие из исходной строки user_input (кавычки сохраняются)  # noqa: E501
            case ["select", "from", table, "where", *_]:
                if table not in metadata:
                    print(f'Ошибка: Таблица "{table}" не существует.')
                    continue
                low = user_input.lower()
                widx = low.find(" where ")
                if widx == -1:
                    print("Некорректное значение: where.")
                    continue
                where_str = user_input[widx + len(" where "):].strip()
                where_clause = parse_where(where_str)
                if where_clause is None:
                    print("Некорректное значение: where. Ожидается формат: поле = значение (строки в кавычках).") # noqa: E501
                    continue
                data = load_table_data(table)
                rows = select(data, where_clause)
                headers = _headers_from_metadata(metadata, table)
                _render_table(rows, headers)

            # SELECT без WHERE
            case ["select", "from", table]:
                if table not in metadata:
                    print(f'Ошибка: Таблица "{table}" не существует.')
                    continue
                data = load_table_data(table)
                rows = select(data, None)
                headers = _headers_from_metadata(metadata, table)
                _render_table(rows, headers)

            # UPDATE: берём SET/WHERE из user_input (кавычки сохраняются)
            case ["update", table, "set", *_]:
                if table not in metadata:
                    print(f'Ошибка: Таблица "{table}" не существует.')
                    continue
                low = user_input.lower()
                sidx = low.find(" set ")
                widx = low.find(" where ", sidx + 1)
                if sidx == -1 or widx == -1:
                    print("Некорректное значение: отсутствует set/where.")
                    continue

                set_str = user_input[sidx + len(" set "): widx].strip()
                where_str = user_input[widx + len(" where "):].strip()

                set_clause = parse_set(set_str)
                where_clause = parse_where(where_str)
                if set_clause is None or where_clause is None:
                    print("Некорректное значение: set/where. Ожидается формат: поле = значение (строки в кавычках).") # noqa: E501
                    continue

                data = load_table_data(table)
                new_data, changed = update(data, set_clause, where_clause)
                if changed > 0:
                    save_table_data(table, new_data)
                    print(f"Обновлено записей: {changed}.")
                else:
                    print("Записи для обновления не найдены.")

            # DELETE: берём WHERE из user_input (кавычки сохраняются)
            case ["delete", "from", table, "where", *_]:
                if table not in metadata:
                    print(f'Ошибка: Таблица "{table}" не существует.')
                    continue
                low = user_input.lower()
                widx = low.find(" where ")
                if widx == -1:
                    print("Некорректное значение: where.")
                    continue
                where_str = user_input[widx + len(" where "):].strip()
                where_clause = parse_where(where_str)
                if where_clause is None:
                    print("Некорректное значение: where. Ожидается формат: поле = значение (строки в кавычках).") # noqa: E501
                    continue

                data = load_table_data(table)
                before = len(data)
                new_data = delete(data, where_clause)
                save_table_data(table, new_data)
                removed = before - len(new_data)
                if removed > 0:
                    print(f"Удалено записей: {removed}.")
                else:
                    print("Записи для удаления не найдены.")

            # INFO: info <table>
            case ["info", table]:
                if table not in metadata:
                    print(f'Ошибка: Таблица "{table}" не существует.')
                    continue
                headers = _headers_from_metadata(metadata, table)
                data = load_table_data(table)
                cols = metadata[table]
                parts = []
                i = 0
                while i < len(cols):
                    parts.append(cols[i])
                    i += 1
                print(f"Таблица: {table}")
                print(f"Столбцы: {', '.join(parts)}")
                print(f"Количество записей: {len(data)}")

            # Неизвестная команда
            case [cmd, *_]:
                print(f"Функции {cmd} нет. Попробуйте снова.")
