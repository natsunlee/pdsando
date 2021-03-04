import json
from pathlib import Path
from abc import ABC, abstractmethod


class Schema:
    def __init__(self, schema=None):
        if isinstance(schema, str) or isinstance(schema, Path):
            with open(schema, "r") as f:
                self._json_obj = json.load(f)
        elif isinstance(schema, dict):
            self._json_obj = schema
        else:
            raise AttributeError(f"Invalid schema type: {type(schema)}")

    def __str__(self):
        return json.dumps(self.json, indent=4)

    @property
    def json(self):
        return self._json_obj

    @property
    def schema(self):
        return self._to_native()

    def save_to_file(self, filename):
        with open(filename, "w") as f:
            json.dump(self.json, f, indent=4)

    @abstractmethod
    def _to_native(self):
        raise NotImplementedError("The to_native() method is not yet implemented.")