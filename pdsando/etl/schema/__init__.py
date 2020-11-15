import jsonschema, json
import pyarrow as pa
import pandas as pd

class Schema:
  
  def __init__(self, schema=None, schema_file=None, pyarrow_schema=None):
    if schema is not None and schema_file is not None:
      raise AttributeError('Please provide only one of `schema` or `schema_file`.')
    
    self._schema = schema
    if schema_file:
      with open(schema_file, 'r') as f:
        self._schema = json.load(f)
  
  def json(self):
    return self._schema
  
  def parquet(self):
    return pa.schema([ self._to_parquet(self._schema['properties'][p], p) for p in self._schema['properties'] ])
  
  def pandas(self):
    return
  
  def from_parquet(self, dtype):
    
    if isinstance(dtype, pa.Schema):
      return {
        'type': 'object',
        'properties': {
          f.name: self.from_parquet(f)
          for f in dtype
        }
      }
    elif isinstance(dtype, pa.Field):
      return self.from_parquet(dtype.type)
    elif isinstance(dtype, pa.ListType):
      return {
        'type': 'array',
        'items': self.from_parquet(dtype.value_type)
      }
    elif isinstance(dtype, pa.StructType):
      return {
        'type': 'object',
        'properties': {
          f.name: self.from_parquet(f)
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
        'format': 'timestampe'
      }
    elif dtype == pa.timestamp('ms'):
      return {
        'type': 'string',
        'format': 'timestamp'
      }
    elif dtype == pa.string():
      return {
        'type': 'string',
        'format': 'timestamp'
      }
    else:
      raise AttributeError('Unknown type: {}'.format(dtype))
  
  def _to_parquet(self, schema, cur_prop=None):
    tp = schema['type']
    ret_type = None
    
    if tp == 'object':
      ret_type = pa.struct([ self._to_parquet(schema['properties'][p], p) for p in schema['properties'] ])
    
    elif tp == 'array':
      ret_type = pa.list_(self._to_parquet(schema['items']))
    
    elif tp == 'string':
      fmt = schema.get('format')
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