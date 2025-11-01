


import time
from typing import Any, Callable


def handle_db_errors(func: Callable) -> Callable:

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print("Ошибка: Файл данных не найден. Возможно, база данных не инициализирована.") # noqa: E501
            return None
        except KeyError as e:
            print(f"Ошибка: Таблица или столбец {e} не найден.")
            return None
        except ValueError as e:
            print(f"Ошибка валидации: {e}")
            return None
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")
            return None
    return wrapper


def confirm_action(action_name: str) -> Callable:
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            answer = input(f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: ').strip().lower() # noqa: E501

            if answer != "y":
                print("Операция отменена.")
                # Возвращаем "как было", если можем определить:
                if args:
                    first = args[0]
                    # drop_table(metadata, table_name) -> вернуть исходные metadata
                    if isinstance(first, dict):
                        return first
                    # delete(table_data, where_clause) -> вернуть исходные данные
                    if isinstance(first, list):
                        return first
                return None
            return func(*args, **kwargs)
        return wrapper
    return decorator



def log_time(func: Callable) -> Callable:
    def wrapper(*args, **kwargs):
        t0 = time.monotonic()
        result = func(*args, **kwargs)
        dt = time.monotonic() - t0
        print(f"Функция {func.__name__} выполнилась за {dt:.3f} секунд.")
        return result
    return wrapper



def create_cacher() -> Callable[[Any, Callable[[], Any]], Any]:
    """
    Возвращает функцию cache_result(key, value_func),
    которая кэширует результат value_func() по ключу key.
    """
    cache = {}

    def cache_result(key, value_func: Callable[[], Any]):
        if key in cache:
            return cache[key]
        value = value_func()
        cache[key] = value
        return value
    return cache_result


