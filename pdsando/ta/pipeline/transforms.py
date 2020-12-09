import pandas as pd
from pdsando.core.wrappers import PipelineStage

class Transform(PipelineStage):
  
  def __init__(self, **kwargs):
    super().__init__(exmsg='Transform failure', desc='Transform')
  
  def _prec(self, df):
    return True

class ColKeep(Transform):
  
  def __init__(self, columns, **kwargs):
    self._columns = columns
    super().__init__()
  
  def _transform(self, df, verbose):
    if verbose:
      print('Keeping only the following columns: "{}"'.format(','.join(self._columns)))
    return df[self._columns]

class Shift(Transform):
  
  def __init__(self, tgt_col, src_col, shift, **kwargs):
    self._src_col = src_col
    self._tgt_col = tgt_col
    self._shift = shift
    super().__init__()
  
  def _transform(self, df, verbose):
    if verbose:
      print('Adding Shifted col "{}"'.format(self._tgt_col))
    df[self._tgt_col] = df[self._src_col].shift(self._shift)
    return df

class ToDateTime(Transform):
  
  def __init__(self, tgt_col, src_col, unit='ms', to_tz='utc', strip_tz=True, **kwargs):
    self._tgt_col = tgt_col
    self._src_col = src_col
    self._unit = unit
    self._to_tz = to_tz
    self._strip_tz = strip_tz
    super().__init__()
  
  def _transform(self, df, verbose):
    if verbose:
      print('Converting epoch timestamp to human readable timestamp for "{}"'.format(self._src_col))
    
    df[self._tgt_col] = pd.to_datetime(df[self._src_col], utc=True, unit=self._unit)
    
    if self._to_tz != 'utc':
      df[self._tgt_col] = df[self._tgt_col].dt.tz_convert(self._to_tz)
    
    if self._strip_tz:
      df[self._tgt_col] = pd.DatetimeIndex(df[self._tgt_col]).tz_localize(None)
    
    return df

class ResetIndex(Transform):
  
  def __init__(self, drop=True, **kwargs):
    self._drop = drop
    super().__init__()
  
  def _transform(self, df, verbose):
    if verbose:
      print('Resetting DataFrame index')
    
    return df.reset_index(drop=self._drop)

class MinVal(Transform):
  
  def __init__(self, tgt_col, col_list, **kwargs):
    self._tgt_col = tgt_col
    self._col_list = col_list
    super().__init__()
  
  def _transform(self, df, verbose):
    if verbose:
      print('Determining minimum value contained in columns: "{}"'.format(self._col_list))
    
    df[self._tgt_col] = df[self._col_list].min(axis=1)
    
    return df

class MaxVal(Transform):
  
  def __init__(self, tgt_col, col_list, **kwargs):
    self._tgt_col = tgt_col
    self._col_list = col_list
    super().__init__()
  
  def _transform(self, df, verbose):
    if verbose:
      print('Determining minimum value contained in columns: "{}"'.format(self._col_list))
    
    df[self._tgt_col] = df[self._col_list].max(axis=1)
    
    return df

class FillMissingTimeFrames(Transform):
  
  def __init__(self, delta, from_time='9:30', to_time='16:00', **kwargs):
    self._delta = delta
    self._from_time = from_time
    self._to_time = to_time
    super().__init__()
  
  def _per_delta(self, delta=1, start_hour=9, start_minute=30, end_hour=16, end_minute=0):
    time_of_day = []
    
    start = start_hour*60 + start_minute
    end = end_hour*60 + end_minute
    cur = start
    
    while cur < end:
      time_of_day.append(cur)
      cur += delta
      
    return pd.DataFrame({ 'time_of_day' : time_of_day }).set_index('time_of_day')
  
  def _transform(self, df, verbose):
    if verbose:
      print('Filling missing timeframes with delta "{}" between {} and {}'.format(
        self._delta,
        self._from_time,
        self._to_time
      ))
    
    dt_ref = self._per_delta(
      delta = self._delta,
      start_hour = int(self._from_time.split(':')[0]),
      start_minute = int(self._from_time.split(':')[1]),
      end_hour = int(self._to_time.split(':')[0]),
      end_minute = int(self._to_time.split(':')[1])
    )
    
    j = dt_ref.join(df.set_index(df.index.hour*60 + df.index.minute), how='left')
    j.fillna(method='ffill')
    
    return j[list(df.columns)]

class SetIndex(Transform):
  
  def __init__(self, new_index, **kwargs):
    self._new_index = new_index
    super().__init__()
  
  def _transform(self, df, verbose):
    if verbose:
      print('Setting new index to: "{}"'.format(self._new_index))
    
    return df.set_index(self._new_index)

class Slice(Transform):
  
  def __init__(self, start=None, end=None, **kwargs):
    self._start = start
    self._end = end
    super().__init__()
  
  def _prec(self, df):
    if not self._start and not self._end:
      return False
    return True
  
  def _transform(self, df, verbose):
    if verbose:
      print('Returning slice from {} to {}"'.format(self._start, self._end))
    
    if self._start and self._end:
      return df[self._start: self._end]
    elif self._start:
      return df[self._start:]
    else:
      return df[:self._end]

class IntradayGroups(Transform):
  
  def __init__(self, group_size=2, open='Open', high='High', low='Low', close='Close', volume='Volume', **kwargs):
    self._group_size = group_size
    self._open = open
    self._high = high
    self._low = low
    self._close = close
    self._volume = volume
    super().__init__()
  
  def _transform(self, df, verbose):
    if verbose:
      print('Grouping every {} records within each day"'.format(self._group_size))
    
    df['_date_group'] = df[self._timestamp].dt.date
    g = df.groupby(df['_date_group']).cumcount() // self._group_size
    
    return df.groupby(['_date_group', g]).agg({
      self._close: 'last',
      self._high: 'max',
      self._low: 'min',
      self._open: 'first',
      self._timestamp: 'min',
      self._volume: 'sum'
    })

class Sort(Transform):
  
  def __init__(self, sort_col=None, **kwargs):
    self._sort_col = sort_col
    super().__init__()
  
  def _transform(self, df, verbose):
    if verbose:
      print('Sorting by value column {}'.format(self._sort_col or 'index'))
    
    if self._sort_col:
      return df.sort_values(by=self._sort_col, inplace=True)
    else:
      return df.sort_index(inplace=True)

class Invert(Transform):
  
  def __init__(self, tgt_col, src_col=None, **kwargs):
    self._tgt_col = tgt_col
    self._src_col = src_col if src_col else tgt_col
    super().__init__()
  
  def _transform(self, df, verbose):
    if verbose:
      print('Muliplying column {} by -1'.format(self._src_col))
    
    df[self._tgt_col] = df[self._src_col] * -1
    return df