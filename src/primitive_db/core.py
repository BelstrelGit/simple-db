# src/primitive_db/core.py

# src/primitive_db/core.py

from .constants import ALLOWED_TYPES
from .decorators import (
    confirm_action,
    create_cacher,
    handle_db_errors,
    log_time,
)
from .utils import load_table_data

_select_cache = create_cacher()


@handle_db_errors
def create_table(metadata, table_name, columns):
    if table_name in metadata:
        print(f'Ошибка: Таблица "{table_name}" уже существует.')
        return metadata

    cols = _parse_columns(columns)
    if cols is None:
        return metadata

    # Проверим, есть ли ID; если нет — добавим в начало
    has_id = False
    i = 0
    while i < len(cols):
        name = cols[i].split(":", 1)[0].strip().lower()
        if name == "id":
            has_id = True
            break
        i += 1
    if not has_id:
        cols.insert(0, "ID:int")

    metadata[table_name] = cols
    cols_msg = ", ".join(cols)
    print(f'Таблица "{table_name}" успешно создана со столбцами: {cols_msg}')
    return metadata


def _parse_columns(columns):
    # Нормализуем вход: режем пробелы/висячие запятые, валидируем name:type
    clean = []
    i = 0
    while i < len(columns):
        col = columns[i].strip().rstrip(",")
        if not col:
            i += 1
            continue
        if ":" not in col:
            print(f"Некорректное значение: {col}. Попробуйте снова.")
            return None
        name, typ = col.split(":", 1)
        name = name.strip()
        typ = typ.strip().lower()
        if not name or typ not in ALLOWED_TYPES:
            print(f"Некорректное значение: {col}. Попробуйте снова.")
            return None
        clean.append(f"{name}:{typ}")
        i += 1
    return clean


@handle_db_errors
@confirm_action("удаление таблицы")
def drop_table(metadata, table_name):
    if table_name not in metadata:
        print(f'Ошибка: Таблица "{table_name}" не существует.')
        return metadata
    del metadata[table_name]
    print(f'Таблица "{table_name}" успешно удалена.')
    return metadata


def list_tables(metadata):
    for name in sorted(metadata.keys()):
        print(f"- {name}")


def insert(metadata: dict, table_name: str, values: list[str]):
    """
    Добавляет запись в таблицу.
    - проверяет наличие таблицы
    - проверяет соответствие количества значений схеме (без ID)
    - приводит типы согласно схеме (int/str/bool)
    - генерирует новый ID и возвращает обновлённый список данных
    """
    # 0) таблица есть?
    if table_name not in metadata:
        print(f'Ошибка: Таблица "{table_name}" не существует.')
        return None

    # 1) схема столбцов (в порядке из metadata)
    raw_cols = metadata[table_name]  # пример: ["ID:int","name:str","age:int","is_active:bool"] # noqa: E501
    schema = []
    for entry in raw_cols:
        name, typ = entry.split(":", 1)
        schema.append((name.strip(), typ.strip().lower()))

    # 2) отбрасываем ID — по ТЗ значения приходят БЕЗ ID
    non_id_schema = [(n, t) for (n, t) in schema if n.lower() != "id"]

    # 3) проверка количества значений
    if len(values) != len(non_id_schema):
        print(
            "Некорректное значение: количество полей не совпадает со схемой. "
            "Попробуйте снова."
        )
        return None

    # 4) загружаем текущие данные (для генерации ID)
    data = load_table_data(table_name)

    # 5) валидация и приведение типов по индексу
    validated_fields = {}
    for i in range(len(values)):
        raw_value = str(values[i]).strip()
        col_name, col_type = non_id_schema[i]

        if col_type not in ALLOWED_TYPES or not col_name:
            print(f"Некорректное значение: {col_name}:{col_type}. Попробуйте снова.")
            return None

        # снять внешние кавычки для строк
        if len(raw_value) >= 2 and raw_value[0] == raw_value[-1] and raw_value[0] in ("'", '"'): # noqa: E501
            raw_value = raw_value[1:-1]

        # привести тип
        if col_type == "int":
            try:
                coerced = int(raw_value)
            except ValueError:
                print(f"Некорректное значение: {values[i]}. Попробуйте снова.")
                return None
        elif col_type == "bool":
            low = raw_value.lower()
            if low == "true":
                coerced = True
            elif low == "false":
                coerced = False
            else:
                print(f"Некорректное значение: {values[i]}. Попробуйте снова.")
                return None
        elif col_type == "str":
            coerced = raw_value
        else:
            print(f"Неизвестный тип данных: {col_type}.")
            return None

        validated_fields[col_name] = coerced

    # 6) новый ID
    existing_ids = [row.get("ID") for row in data if isinstance(row.get("ID"), int)]
    new_id = (max(existing_ids) + 1) if existing_ids else 1

    # 7) запись строго в порядке из non_id_schema
    record = {"ID": new_id}
    for col_name, _ in non_id_schema:
        record[col_name] = validated_fields[col_name]

    data.append(record)
    return data


def _row_fingerprint(row: dict) -> tuple:
    # стабильное представление строки: пары (ключ, значение), отсортированы по ключу
    # значения у нас базовых типов (int/bool/str), они хэшируемые
    return tuple(sorted(row.items(), key=lambda kv: kv[0]))

def _table_fingerprint(rows: list[dict]) -> tuple:
    # длина + отсортированный набор "отпечатков" строк
    # порядок строк в файле не важен — сортируем
    row_fps = []
    i = 0
    while i < len(rows):
        row_fps.append(_row_fingerprint(rows[i]))
        i += 1
    row_fps.sort()
    return (len(rows), tuple(row_fps))


@handle_db_errors
@log_time
def select(table_data, where_clause=None):
    # Ключ кэша: тип операции + полный отпечаток таблицы + условие
    where_key = None
    if where_clause is not None:
        where_items = sorted(where_clause.items())
        where_key = tuple(where_items)

    key = ("select", _table_fingerprint(table_data), where_key)

    def _compute():
        if where_clause is None:
            # можно вернуть копию, чтобы не делиться ссылкой на исходный список
            return list(table_data)

        filtered = []
        i = 0
        while i < len(table_data):
            row = table_data[i]
            match_row = True
            for k, v in where_clause.items():
                if k not in row or row[k] != v:
                    match_row = False
                    break
            if match_row:
                filtered.append(row)
            i += 1
        return filtered

    return _select_cache(key, _compute)



@handle_db_errors
def update(table_data, set_clause, where_clause):
    updated_data = []
    updated_count = 0
    i = 0
    while i < len(table_data):
        row = table_data[i]

        # Проверяем соответствие where_clause
        match_row = True
        for key, value in where_clause.items():
            if key not in row or row[key] != value:
                match_row = False
                break

        if match_row:
            # Обновляем поля согласно set_clause
            for k, v in set_clause.items():
                row[k] = v
            updated_count += 1

        updated_data.append(row)
        i += 1

    return updated_data, updated_count


@handle_db_errors
@confirm_action("удаление записей")
def delete(table_data, where_clause):
    new_data = []
    removed = 0
    i = 0
    while i < len(table_data):
        row = table_data[i]

        match_row = True
        for key, value in where_clause.items():
            if key not in row or row[key] != value:
                match_row = False
                break

        if not match_row:
            new_data.append(row)
        else:
            removed += 1

        i += 1

    return new_data
