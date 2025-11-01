# src/primitive_db/engine.py

import shlex

from prettytable import PrettyTable

from .constants import META_FILE
from .core import (
    create_table,
    delete,
    drop_table,
    insert,
    list_tables,
    select,
    update,
)
from .decorators import handle_db_errors
from .parser import parse_set, parse_where
from .utils import (
    load_metadata,
    load_table_data,
    save_metadata,
    save_table_data,
)


def print_help() -> None:
    """Печатает справку по командам."""
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print(
        "<command> create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> .. - "
        "создать таблицу"
    )
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")

    print("\n***Операции с данными***")
    print(
        "<command> insert into <имя_таблицы> values (<v1>, <v2>, ...) - создать запись"
    )
    print(
        "<command> select from <имя_таблицы> [where <col> = <value>] - прочитать записи"
    )
    print(
        "<command> update <имя_таблицы> set <col> = <value> where <col> = <value> - "
        "обновить записи"
    )
    print("<command> delete from <имя_таблицы> where <col> = <value> - удалить записи")
    print("<command> info <имя_таблицы> - информация о таблице")

    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")


@handle_db_errors
def _split_args(s: str) -> list[str]:
    """Разбор строки через shlex.split. Ошибки ловит декоратор."""
    return shlex.split(s)


def _split_values_inner(inner: str) -> list[str]:
    """
    Разбивает строку внутри VALUES(...), учитывая кавычки и запятые.
    Пример: '"Sergei", 28, true' -> ['\"Sergei\"', '28', 'true'].
    """
    parts: list[str] = []
    token: list[str] = []
    in_quote: str | None = None  # None | "'" | '"'

    i = 0
    while i < len(inner):
        ch = inner[i]

        if ch in ("'", '"'):
            if in_quote is None:
                in_quote = ch
                token.append(ch)
            elif in_quote == ch:
                in_quote = None
                token.append(ch)
            else:
                # внутри "..." встретили ' — это просто символ
                token.append(ch)
        elif ch == "," and in_quote is None:
            # запятая вне кавычек — разделитель
            val = "".join(token).strip()
            if val != "":
                parts.append(val)
            token = []
        else:
            token.append(ch)

        i += 1

    # хвост
    val = "".join(token).strip()
    if val != "":
        parts.append(val)

    return parts


def _find_keyword_outside_quotes(s: str, keyword: str) -> int:
    """Ищет позицию keyword (вкл. пробелы, напр. ' where ') вне кавычек."""
    kw = keyword.lower()
    low = s.lower()
    in_quote: str | None = None
    i = 0
    n = len(s)
    m = len(kw)

    while i <= n - m:
        ch = s[i]
        if ch in ("'", '"'):
            if in_quote is None:
                in_quote = ch
            elif in_quote == ch:
                in_quote = None
            i += 1
            continue

        if in_quote is None and low.startswith(kw, i):
            return i
        i += 1
    return -1


def _extract_condition_after_where(user_input: str) -> str | None:
    """Возвращает подстроку после первого ' where ' вне кавычек, либо None."""
    idx = _find_keyword_outside_quotes(user_input, " where ")
    if idx == -1:
        return None
    return user_input[idx + len(" where ") :].strip()


def _extract_update_clauses(user_input: str) -> tuple[str, str] | None:
    """
    Извлекает set-часть и where-часть из исходной строки update ... set ... where ...
    Возвращает (set_str, where_str) или None при ошибке.
    """
    idx_set = _find_keyword_outside_quotes(user_input, " set ")
    idx_where = _find_keyword_outside_quotes(user_input, " where ")
    if idx_set == -1 or idx_where == -1 or idx_where < idx_set:
        return None
    set_str = user_input[idx_set + len(" set ") : idx_where].strip()
    where_str = user_input[idx_where + len(" where ") :].strip()
    return (set_str, where_str)


def _print_rows(table: str, metadata: dict, rows: list[dict]) -> None:
    """Красивый вывод записей таблицы с учётом порядка колонок из схемы."""
    if not rows:
        headers = [c.split(":", 1)[0] for c in metadata.get(table, [])]
        if not headers:
            print("(нет записей)")
            return
        t = PrettyTable()
        t.field_names = headers
        print(t)
        return

    headers = [c.split(":", 1)[0] for c in metadata.get(table, [])] or list(
        rows[0].keys()
    )
    t = PrettyTable()
    t.field_names = headers
    for r in rows:
        t.add_row([r.get(h) for h in headers])
    print(t)


def run():
    while True:
        metadata = load_metadata(META_FILE)
        user_input = input("Введите команду: ").strip()
        if not user_input:
            continue

        args = _split_args(user_input)
        if args is None:  # декоратор вернёт None, если был ValueError в shlex
            continue

        match args:
            case ["help"]:
                print_help()

            case ["exit"]:
                break

            case ["list_tables"]:
                list_tables(metadata)

            case ["create_table"]:
                print(
                    "Некорректное значение: отсутствует имя таблицы. "
                    "Попробуйте снова."
                )
                continue

            case ["create_table", table, *cols]:
                metadata = create_table(metadata, table, cols)
                save_metadata(META_FILE, metadata)
                continue

            case ["drop_table"]:
                print(
                    "Некорректное значение: отсутствует имя таблицы. "
                    "Попробуйте снова."
                )
                continue

            case ["drop_table", table]:
                metadata = drop_table(metadata, table)
                save_metadata(META_FILE, metadata)
                continue

            # INSERT: insert into <table> values (...)
            case ["insert", "into", table, *rest]:
                if not rest or rest[0].lower() != "values":
                    print(
                        "Некорректное значение: ожидается 'values (...)'. "
                        "Попробуйте снова."
                    )
                    continue

                start = user_input.find("(")
                end = user_input.rfind(")")
                if start == -1 or end == -1 or end < start:
                    print(
                        "Некорректное значение: отсутствует список значений в скобках. "
                        "Попробуйте снова."
                    )
                    continue

                inner = user_input[start + 1 : end]
                values = _split_values_inner(inner)

                before = load_table_data(table)
                after = insert(metadata, table, values)
                if after is None:
                    continue
                save_table_data(table, after)

                new_id = None
                if len(after) > len(before):
                    last = after[-1]
                    if isinstance(last, dict) and "ID" in last:
                        new_id = last["ID"]

                if new_id is not None:
                    print(
                        f'Запись с ID={new_id} успешно добавлена в таблицу "{table}".'
                    )
                else:
                    print(f'Запись успешно добавлена в таблицу "{table}".')
                continue

            # SELECT: select from <table> [where <col> = <value>]
            case ["select", "from", table, "where", *_]:
                # берём условие из исходной строки, чтобы не терять кавычки
                cond_str = _extract_condition_after_where(user_input)
                if not cond_str:
                    print(
                        "Некорректное значение: where. "
                        "Ожидается формат: поле = значение (строки в кавычках)."
                    )
                    continue
                where_clause = parse_where(cond_str)
                if where_clause is None:
                    print(
                        "Некорректное значение: where. "
                        "Ожидается формат: поле = значение (строки в кавычках)."
                    )
                    continue

                data = load_table_data(table)
                rows = select(data, where_clause)
                _print_rows(table, metadata, rows)
                continue

            case ["select", "from", table]:
                data = load_table_data(table)
                rows = select(data, None)
                _print_rows(table, metadata, rows)
                continue

            # UPDATE: update <table> set <col>=<value> where <col>=<value>
            case ["update", table, "set", *rest]:
                # извлекаем set/where из исходной строки, сохраняя кавычки
                clauses = _extract_update_clauses(user_input)
                if clauses is None:
                    print(
                        "Некорректное значение: отсутствует корректная секция "
                        "SET/WHERE. Попробуйте снова."
                    )
                    continue
                set_str, where_str = clauses

                set_clause = parse_set(set_str)
                where_clause = parse_where(where_str)
                if set_clause is None or where_clause is None:
                    print(
                        "Некорректное значение: set/where. "
                        "Ожидается формат: поле = значение (строки в кавычках)."
                    )
                    continue

                data = load_table_data(table)
                new_data, changed = update(data, set_clause, where_clause)
                if new_data is None:
                    continue
                save_table_data(table, new_data)
                if changed > 0:
                    print(f"Обновлено записей: {changed}.")
                else:
                    print("Записи для обновления не найдены.")
                continue

            # DELETE: delete from <table> where <col>=<value>
            case ["delete", "from", table, "where", *_]:
                cond_str = _extract_condition_after_where(user_input)
                if not cond_str:
                    print(
                        "Некорректное значение: where. "
                        "Ожидается формат: поле = значение (строки в кавычках)."
                    )
                    continue

                where_clause = parse_where(cond_str)
                if where_clause is None:
                    print(
                        "Некорректное значение: where. "
                        "Ожидается формат: поле = значение (строки в кавычках)."
                    )
                    continue

                data = load_table_data(table)
                before_len = len(data)
                new_data = delete(data, where_clause)
                if new_data is None:
                    continue
                removed = before_len - len(new_data)
                save_table_data(table, new_data)
                if removed > 0:
                    print(f"Удалено записей: {removed}.")
                else:
                    print("Записи для удаления не найдены.")
                continue

            # INFO: info <table>
            case ["info", table]:
                if table not in metadata:
                    print(f'Ошибка: Таблица "{table}" не существует.')
                    continue
                cols_msg = ", ".join(metadata[table])
                count = len(load_table_data(table))
                print(f"Таблица: {table}")
                print(f"Столбцы: {cols_msg}")
                print(f"Количество записей: {count}")
                continue

            # нераспознанная команда
            case [cmd, *_]:
                print(f"Функции {cmd} нет. Попробуйте снова.")
                continue

            case _:
                print("Некорректная команда. Введите 'help' для справки.")
                continue
