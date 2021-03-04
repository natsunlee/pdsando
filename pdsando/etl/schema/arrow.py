import pyarrow as pa
from pdsando.etl.schema.abstract import Schema


class ArrowSchema(Schema):
    def _to_native(self):
        return pa.schema(
            [
                self._from_json_property(self.json["properties"][p], p)
                for p in self.json["properties"]
            ]
        )

    def _from_json_property(self, json_obj, cur_prop=None):
        tp = json_obj["type"]
        ret_type = None

        if tp == "object":
            ret_type = pa.struct(
                [
                    self._from_json_property(json_obj["properties"][p], p)
                    for p in json_obj["properties"]
                ]
            )

        elif tp == "array":
            ret_type = pa.list_(self._from_json_property(json_obj["items"]))

        elif tp == "string":
            fmt = json_obj.get("format")
            if fmt == "date":
                ret_type = pa.date32()
            elif fmt == "time":
                ret_type = pa.time64()
            elif fmt == "date-time":
                ret_type = pa.timestamp("ns")
            else:
                ret_type = pa.string()

        elif tp == "integer":
            ret_type = pa.int64()

        elif tp == "number":
            ret_type = pa.float64()

        elif tp == "bool":
            ret_type = pa.bool_()

        else:
            raise AttributeError(f"Unknown type: {tp}")

        if cur_prop:
            return (cur_prop, ret_type, True)
        else:
            return ret_type