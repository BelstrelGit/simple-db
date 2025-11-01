# src/primitive_db/core.py

from .utils import load_table_data  # load_metadata тут не нужен

ALLOWED_TYPES = {"int", "str", "bool"}


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


def insert(metadata, table_name, values):
    # 1) Таблица существует?
    if table_name not in metadata:
        print(f'Ошибка: Таблица "{table_name}" не существует.')
        return None

    # 2) Построить схему [(name, type), ...] из metadata
    raw_cols = metadata[table_name]
    schema = []
    i = 0
    while i < len(raw_cols):
        entry = raw_cols[i]
        if ":" not in entry:
            print(f"Некорректное значение: {entry}. Попробуйте снова.")
            return None
        col_name, col_type = entry.split(":", 1)
        schema.append((col_name.strip(), col_type.strip().lower()))
        i += 1

    # 3) Схема без ID
    non_id_schema = []
    i = 0
    while i < len(schema):
        n, t = schema[i]
        if n.strip().lower() != "id":
            non_id_schema.append((n, t))
        i += 1

    # 4) Кол-во значений совпадает?
    if len(values) != len(non_id_schema):
        print("Некорректное значение: количество полей не совпадает со схемой. Попробуйте снова.") # noqa: E501
        return None

    # 5) Загрузить текущее содержимое таблицы
    data = load_table_data(table_name)

    # 6) Провести валидацию и приведение типов
    validated_fields = {}
    k = 0
    while k < len(values):
        raw_value = str(values[k]).strip()
        col_name, col_type = non_id_schema[k]

        if col_type not in ALLOWED_TYPES or not col_name:
            print(f"Некорректное значение: {col_name}:{col_type}. Попробуйте снова.")
            return None

        # снять внешние кавычки, если есть
        if len(raw_value) >= 2 and raw_value[0] == raw_value[-1] and raw_value[0] in ("'", '"'): # noqa: E501
            raw_value = raw_value[1:-1]

        match col_type:
            case "int":
                try:
                    coerced = int(raw_value)
                except ValueError:
                    print(f"Некорректное значение: {values[k]}. Попробуйте снова.")
                    return None
            case "bool":
                low = raw_value.lower()
                if low == "true":
                    coerced = True
                elif low == "false":
                    coerced = False
                else:
                    print(f"Некорректное значение: {values[k]}. Попробуйте снова.")
                    return None
            case "str":
                coerced = raw_value
            case _:
                print(f"Неизвестный тип данных: {col_type}.")
                return None

        validated_fields[col_name] = coerced
        k += 1

    # 7) Сгенерировать новый ID
    existing_ids = []
    i = 0
    while i < len(data):
        rid = data[i].get("ID")
        if isinstance(rid, int):
            existing_ids.append(rid)
        i += 1
    new_id = (max(existing_ids) + 1) if existing_ids else 1

    # 8) Собрать запись и добавить в таблицу
    record = {"ID": new_id}
    p = 0
    while p < len(non_id_schema):
        col_name, _ = non_id_schema[p]
        record[col_name] = validated_fields[col_name]
        p += 1

    data.append(record)
    return data


def select(table_data, where_clause=None):
    if where_clause is None:
        return table_data

    filtered = []
    i = 0
    while i < len(table_data):
        row = table_data[i]
        match_row = True
        for key, value in where_clause.items():
            if key not in row or row[key] != value:
                match_row = False
                break
        if match_row:
            filtered.append(row)
        i += 1
    return filtered


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

def delete(table_data, where_clause):
    new_data = []
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

        i += 1

    return new_data
