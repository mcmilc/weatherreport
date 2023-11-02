import json


def read_info(filename: str) -> dict:
    return json.load(open(filename))
