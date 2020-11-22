from pandas import DataFrame

class PriceData(DataFrame):
  def __init__(self, *args, **kwargs):
    # Init Pandas DataFrame
    super().__init__(*args, **{ k:kwargs[k] for k in kwargs if k not in ['timespan', 'multiplier', 'source', 'category'] })
    
    # Store additional details specific to PriceData
    self.timespan = kwargs['timespan']
    self.multiplier = kwargs['multiplier']
    self.source = kwargs['source']
    self.category = kwargs.get('category', 'stocks')
  
  @property
  def timespan(self): return self._timespan
  @timespan.setter
  def timespan(self, value):
    valid_timespans = ('second', 'minute', 'hour', 'day', 'month', 'year')
    if value not in valid_timespans:
      raise ValueError(f'Timespan must be one of: {valid_timespans}')
    self._timespan = value
  
  @property
  def multiplier(self): return self._multiplier
  @multiplier.setter
  def multiplier(self, value):
    if not isinstance(value, int):
      raise TypeError(f'Multiplier must be an integer, not {type(value)}')
    if int(value) <= 0:
      raise ValueError(f'Multiplier must be a valid integer greater than 0')
    self._multiplie = int(value)
  
  @property
  def source(self): return self._source
  @source.setter
  def source(self, value):
    self._source = value
  
  @property
  def category(self): return self._category
  @category.setter
  def category(self, value):
    valid_categories = ('stocks', 'forex', 'crypto')
    if value not in valid_categories:
      raise ValueError(f'Category must be one of: {valid_categories}')
    self._category = value
  
  def copy(self, **kwargs):
    return PriceData(super().copy(**kwargs), timespan=self._timespan, multiplier=self._multiplier, source=self._source)
  
  # Up/downscale data resolution
  def match_resolution(self, model_data):
    if model_data.timespan != self._timespan:
      raise NotImplementedError('Currently cannot support matching different timespan (source: {} | target: {})'.format(self._timespan, model_data.timespan))
    if model_data.multiplier == self._multiplier:
      return
    
    if model_data.multiplier > self._multiplier:
      temp = self.join(model_data.set_index('Timestamp'), on='Timestamp', rsuffix='_model', how='inner')
    else:
      temp = self.join(model_data.set_index('Timestamp'), on='Timestamp', rsuffix='_model', how='right').sort_values(by='Timestamp').fillna(method='ffill')
    
    return temp[list(self.columns)].copy().sort_values(by='Timestamp').reset_index(drop=True)