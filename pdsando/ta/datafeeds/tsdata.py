import pandas as pd


def to_time_series_data(df, timespan, multiplier, index_col=None, source=None, category=None):
    temp = df.copy()
    ts_vals = temp[index_col] if index_col else temp.index.to_series()
    idx_name = index_col or temp.index.name

    temp[idx_name] = match_to_resolution(
        ts_vals, Resolution(timespan, multiplier))
    temp.set_index(idx_name, inplace=True)

    return TimeSeriesData(temp, timespan=timespan, multiplier=multiplier, source=source, category=category)


def match_to_resolution(ts_series, resolution):
    seconds_from_epoch = (
        ts_series - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
    seconds_of_day = 3600*ts_series.dt.hour + 60 * \
        ts_series.dt.minute + ts_series.dt.second

    if resolution.timespan == 'second':
        seconds_to_delete = seconds_of_day % resolution.multiplier
    elif resolution.timespan == 'minute':
        seconds_to_delete = seconds_of_day % (resolution.multiplier * 60)
    elif resolution.timespan == 'hour':
        seconds_to_delete = seconds_of_day % (resolution.multiplier * 3600)
    elif resolution.timespan == 'day':
        seconds_to_delete = (((ts_series.dt.day-1) %
                              resolution.multiplier) * 86400) + seconds_of_day
    # elif timespan == 'week':
    #  seconds_to_delete = (((ts_series.dt.week-1) % resolution.multiplier) * 604800) + seconds_of_day
    else:
        raise ValueError(
            'timespan may only be one of: second, minute, hour, day, week')

    return pd.to_datetime((seconds_from_epoch - seconds_to_delete), unit='s', origin='unix')


class TimeSeriesData(pd.DataFrame):

    def __init__(self, *args, **kwargs):
        # Init Pandas DataFrame
        super().__init__(*args, **
                         {k: kwargs[k] for k in kwargs if k not in ['timespan', 'multiplier', 'source', 'category']})
        self.sort_index(inplace=True)

        # Store additional details specific to PriceData
        self._source = self._category = self._ts_col = None
        self._resolution = Resolution(kwargs['timespan'], kwargs['multiplier'])
        self.source = kwargs.get('source') or 'unknown'
        self.category = kwargs.get('category') or 'stocks'

    @property
    def timespan(self): return self._resolution.timespan
    @property
    def multiplier(self): return self._resolution.multiplier
    @property
    def resolution(self): return self._resolution

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

    # Ensure DataFrame copy() returns another TimeSeriesData obj
    def copy(self, **kwargs):
        return TimeSeriesData(super().copy(**kwargs), timespan=self.timespan, multiplier=self.multiplier, source=self._source, category=self._category)

    def supplement(self, sup_data):
        if not isinstance(sup_data, TimeSeriesData):
            raise AttributeError(
                'Supplementary data must be of type TimeSeriesData')

        if sup_data.resolution > self.resolution:
            sup_data['_join_ts'] = match_to_resolution(
                sup_data.index.to_series(), self.resolution)
            sup_data['_rank'] = sup_data.index.to_series().groupby(
                sup_data['_join_ts']).rank(method='first', ascending=True)
            temp = self.join(sup_data[sup_data['_rank'] == 1.0].set_index(
                '_join_ts'), rsuffix='_model', how='left')
        elif sup_data.resolution < self.resolution:
            self['_join_ts'] = match_to_resolution(
                self.index.to_series(), sup_data.resolution)
            temp = self.join(sup_data, on='_join_ts',
                             rsuffix='_model', how='left')
        else:
            temp = self.join(sup_data, rsuffix='_model', how='left')

        return temp.drop(['_join_ts', '_rank'], errors='ignore', axis=1)

    # Up/downscale data resolution
    def match_resolution(self, model_data):
        if not isinstance(model_data, TimeSeriesData):
            raise AttributeError('Model data must be of type TimeSeriesData')

        if model_data.timespan != self.timespan:
            raise NotImplementedError('Currently cannot support matching different timespan (source: {} | target: {})'.format(
                self.timespan, model_data.timespan))
        if model_data.multiplier == self.multiplier:
            return

        if model_data.multiplier > self.multiplier:
            temp = self.join(model_data, rsuffix='_model', how='inner')
        else:
            temp = self.join(model_data, rsuffix='_model',
                             how='right').fillna(method='ffill')

        return TimeSeriesData(temp[list(self.columns)].copy(), timespan=self.timespan, multiplier=self.multiplier, source=self._source, category=self._category)


class Resolution:

    valid_timespans = {
        'second': 5,
        'minute': 4,
        'hour': 3,
        'day': 2,
        'week': 1
    }

    def __init__(self, timespan, multiplier):
        self._timespan = self._multiplier = None
        self.timespan = timespan
        self.multiplier = multiplier

    @property
    def timespan(self): return self._timespan

    @timespan.setter
    def timespan(self, value):
        vt = list(Resolution.valid_timespans.keys())
        if value not in vt:
            raise ValueError(f'Timespan must be one of: {vt}')
        self._timespan = value

    @property
    def multiplier(self): return self._multiplier

    @multiplier.setter
    def multiplier(self, value):
        if not isinstance(value, int):
            raise TypeError(
                f'Multiplier must be an integer, not {type(value)}')
        if int(value) <= 0:
            raise ValueError(
                f'Multiplier must be a valid integer greater than 0')
        if value in ('second', 'minute') and int(value) > 60:
            raise ValueError(
                f'Multiplier must be between 1 and 60 for timespan: {self._timespan}')
        elif value == 'hour' and int(value) > 24:
            raise ValueError(
                f'Multiplier must be between 1 and 24 for timespan: {self._timespan}')
        self._multiplier = int(value)

    def __lt__(self, other):
        return (
            Resolution.valid_timespans[self.timespan] < Resolution.valid_timespans[other.timespan]
            or
            (
                Resolution.valid_timespans[self.timespan] == Resolution.valid_timespans[other.timespan]
                and
                # Higher multiplier means lower resolution
                self.multiplier > other.multiplier
            )
        )

    def __le__(self, other):
        return (
            Resolution.valid_timespans[self.timespan] < Resolution.valid_timespans[other.timespan]
            or
            (
                Resolution.valid_timespans[self.timespan] == Resolution.valid_timespans[other.timespan]
                and
                # Higher multiplier means lower resolution
                self.multiplier >= other.multiplier
            )
        )

    def __eq__(self, other):
        return (
            Resolution.valid_timespans[self.timespan] == Resolution.valid_timespans[other.timespan]
            and
            self.multiplier == other.multiplier
        )

    def __ne__(self, other):
        return (
            Resolution.valid_timespans[self.timespan] != Resolution.valid_timespans[other.timespan]
            or
            self.multiplier != other.multiplier
        )

    def __gt__(self, other):
        return (
            Resolution.valid_timespans[self.timespan] > Resolution.valid_timespans[other.timespan]
            or
            (
                Resolution.valid_timespans[self.timespan] == Resolution.valid_timespans[other.timespan]
                and
                # Lower multiplier means higher resolution
                self.multiplier < other.multiplier
            )
        )

    def __ge__(self, other):
        return (
            Resolution.valid_timespans[self.timespan] > Resolution.valid_timespans[other.timespan]
            or
            (
                Resolution.valid_timespans[self.timespan] == Resolution.valid_timespans[other.timespan]
                and
                # Lower multiplier means higher resolution
                self.multiplier <= other.multiplier
            )
        )
