import jsonschema, json
import pyarrow as pa
import pandas as pd
from pathlib import Path

class Schema:
  
  def __init__(self, schema=None):
    if isinstance(schema, pa.Schema):
      self._schema = schema
    elif isinstance(schema, str) or isinstance(schema, Path):
      self._schema = self._from_file(schema)
    elif isinstance(schema, dict):
      self._schema = self._from_json(schema)
    else:
      raise AttributeError('Invalid schema type: {}'.format(type(schema)))
  
  def __str__(self):
    return json.dumps(self.json, indent=4)
  
  @property
  def json(self):
    return self._to_json(self._schema)
  @property
  def pyarrow(self):
    return self._schema
  
  def save_to_file(self, filename):
    with open(filename, 'w') as f:
      json.dump(self.json, f, indent=4)
  
  def _from_file(self, filename):
    with open(filename, 'r') as f:
      json_obj = json.load(f)
    return self._from_json(json_obj)
  
  def _from_json(self, json_obj):
    return pa.schema([ self._from_json_property(json_obj['properties'][p], p) for p in json_obj['properties'] ])
  
  def _from_json_property(self, json_obj, cur_prop=None):
    tp = json_obj['type']
    ret_type = None
    
    if tp == 'object':
      ret_type = pa.struct([ self._from_json_property(json_obj['properties'][p], p) for p in json_obj['properties'] ])
    
    elif tp == 'array':
      ret_type = pa.list_(self._from_json_property(json_obj['items']))
    
    elif tp == 'string':
      fmt = json_obj.get('format')
      if fmt == 'date':
        ret_type = pa.date32()
      elif fmt == 'time':
        ret_type = pa.time64()
      elif fmt == 'date-time':
        ret_type = pa.timestamp('ns')
      else:
        ret_type = pa.string()
    
    elif tp == 'integer':
      ret_type = pa.int64()
    
    elif tp == 'number':
      ret_type = pa.float64()
    
    elif tp == 'bool':
      ret_type = pa.bool_()
    
    else:
      raise AttributeError('Unknown type: {}'.format(tp))
    
    if cur_prop:
      return (cur_prop, ret_type, True)
    else:
      return ret_type
  
  def _to_json(self, dtype):
    
    if isinstance(dtype, pa.Schema):
      return {
        'type': 'object',
        'properties': {
          f.name: self._to_json(f)
          for f in dtype
        }
      }
    elif isinstance(dtype, pa.Field):
      return self._to_json(dtype.type)
    elif isinstance(dtype, pa.ListType):
      return {
        'type': 'array',
        'items': self._to_json(dtype.value_type)
      }
    elif isinstance(dtype, pa.StructType):
      return {
        'type': 'object',
        'properties': {
          f.name: self._to_json(f)
          for f in dtype
        }
      }
    elif dtype == pa.int32():
      return { 'type': 'integer' }
    elif dtype == pa.int64():
      return { 'type': 'integer' }
    elif dtype == pa.float32():
      return { 'type': 'number' }
    elif dtype == pa.float64():
      return { 'type': 'number' }
    elif dtype == pa.bool_():
      return { 'type': 'bool' }
    elif dtype == pa.date32():
      return {
        'type': 'string',
        'format': 'date'
      }
    #elif tp == pa.time64():
    #  ret_type = {
    #    'type': 'string',
    #    'format': 'date'
    #  }
    elif dtype == pa.timestamp('ns'):
      return {
        'type': 'string',
        'format': 'date-time'
      }
    elif dtype == pa.timestamp('ms'):
      return {
        'type': 'string',
        'format': 'date-time'
      }
    elif dtype == pa.string():
      return {
        'type': 'string'
      }
    else:
      raise AttributeError('Unknown type: {}'.format(dtype))