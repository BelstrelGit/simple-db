# src/primitive_db/utils.py
import json
import os

META_FILE = "db_meta.json"
DATA_DIR_NAME = "data"


def _data_dir():
    base_dir = os.path.dirname(os.path.abspath(META_FILE))
    data_dir = os.path.join(base_dir, DATA_DIR_NAME)
    if not os.path.isdir(data_dir):
        os.makedirs(data_dir, exist_ok=True)
    return data_dir


def _table_path(table_name: str) -> str:
    return os.path.join(_data_dir(), f"{table_name}.json")


def load_metadata(filepath: str = META_FILE):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_metadata(filepath: str, data):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_table_data(table_name: str):
    path = _table_path(table_name)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def save_table_data(table_name: str, data):
    path = _table_path(table_name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
