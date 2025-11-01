


def parse_condition_strict(s: str):
    # ожидаем ровно: <col> = <value>, где строковые значения в кавычках
    s = s.strip()
    parts = s.split("=")

    match len(parts):
        case 2:
            key = parts[0].strip()
            raw = parts[1].strip()
        case _:
            return None

    # строка в кавычках?
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in ('"', "'"):
        return {key: raw[1:-1]}

    # bool / int / ошибка
    low = raw.lower()
    match low:
        case "true":
            return {key: True}
        case "false":
            return {key: False}
        case _:
            try:
                return {key: int(raw)}
            except ValueError:
                # по правилу: строки должны быть в кавычках
                return None


def parse_where(where_str: str):
    return parse_condition_strict(where_str)


def parse_set(set_str: str):
    return parse_condition_strict(set_str)
