



ALLOWED_TYPES = {"int", "str", "bool"}


def create_table(metadata, table_name, columns):
    if table_name in metadata:
        print(f'Ошибка: Таблица "{table_name}" уже существует.')
        return metadata

    cols = _parse_columns(columns)
    if cols is None:
        return metadata

    has_id = any(col.split(":", 1)[0].lower() == "id" for col in cols)
    if not has_id:
        cols.insert(0, "ID:int")

    metadata[table_name] = cols
    cols_msg = ", ".join(cols)
    print(f'Таблица "{table_name}" успешно создана со столбцами: {cols_msg}')
    return metadata


def _parse_columns(columns):
    cols = []
    for col in columns:
        if ":" not in col:
            print(f"Некорректное значение: {col}. Попробуйте снова.")
            return None
        name, typ = col.split(":", 1)
        name = name.strip()
        typ = typ.strip().lower()
        if typ not in ALLOWED_TYPES or not name:
            print(f"Некорректное значение: {col}. Попробуйте снова.")
            return None
        cols.append(f"{name}:{typ}")
    return cols


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
