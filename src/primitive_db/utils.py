
import json

'''Загружает данные из JSON-файла. Если файл не найден, 
возвращает пустой словарь {}. 
Используйте try...except FileNotFoundError.'''
def load_metadata(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    # except json.JSONDecodeError:
    #     return {}


'''Сохраняет переданные данные в JSON-файл.'''
def save_metadata(filepath, data):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

