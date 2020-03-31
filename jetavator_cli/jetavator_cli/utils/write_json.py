import json


def write_json(data, path):
    with open(path, 'w') as json_file:
        json.dump(data, json_file, indent=2)
