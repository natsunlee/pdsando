import pyspark.sql.types as pst
from pdsando.etl.schema.abstract import Schema


class SparkSchema(Schema):
    def _to_native(self):
        return pst.StructType(
            [
                self._from_json_property(self.json["properties"][p], p)
                for p in self.json["properties"]
            ]
        )

    def _from_json_property(self, json_obj, cur_prop=None):
        tp = json_obj["type"]
        ret_type = None

        if tp == "object":
            ret_type = pst.StructType(
                [
                    self._from_json_property(json_obj["properties"][p], p)
                    for p in json_obj["properties"]
                ]
            )

        elif tp == "array":
            ret_type = pst.ArrayType(self._from_json_property(json_obj["items"]))

        elif tp == "string":
            fmt = json_obj.get("format")
            if fmt == "date":
                ret_type = pst.DateType()
            elif fmt == "date-time":
                ret_type = pst.TimestampType()
            else:
                ret_type = pst.StringType()

        elif tp == "integer":
            ret_type = pst.IntegerType()

        elif tp == "number":
            ret_type = pst.FloatType()

        elif tp == "bool":
            ret_type = pst.BooleanType()

        else:
            raise AttributeError(f"Unknown type: {tp}")

        if cur_prop:
            return pst.StructField(cur_prop, ret_type, True)
        else:
            return ret_type