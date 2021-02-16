import pdpipe as pdp
import numpy as np
import pandas as pd
import mplfinance as mpf
from pdsando.core.wrappers import PipelineStage, Pipeline


class Indicator(PipelineStage):

    def __init__(self, **kwargs):
        self._tgt_col = kwargs.pop('tgt_col')
        self._color = kwargs.pop('color', 'black')
        self._width = kwargs.pop('width', 1)
        self._alpha = kwargs.pop('alpha', 1)
        self._secondary = kwargs.pop('secondary', False)
        super().__init__(exmsg='Indicator failure', desc='Indicator')

    @property
    def secondary(self): return self._secondary

    def _prec(self, df):
        return True

    def _get_or_apply(self, df):
        if self._tgt_col in df.columns:
            return df
        else:
            return self._transform(df, False)

    def _indicator(self, df, panel=0):
        return [mpf.make_addplot(self._get_or_apply(df)[self._tgt_col], panel=panel, color=self._color, type='line', width=self._width, alpha=self._alpha)]


class SMA(Indicator):

    def __init__(self, tgt_col, src_col, period=5, **kwargs):
        self._tgt_col = tgt_col
        self._src_col = src_col
        self._period = period
        super().__init__(tgt_col=tgt_col, **kwargs)

    def _transform(self, df, verbose):
        if verbose:
            print('Determining Simple Moving Average for: "{}"'.format(self._src_col))
        df[self._tgt_col] = df[self._src_col].rolling(self._period).mean()
        return df


class EMA(Indicator):

    def __init__(self, tgt_col, src_col, period=5, **kwargs):
        self._src_col = src_col
        self._tgt_col = tgt_col
        self._period = period
        super().__init__(tgt_col=tgt_col, **kwargs)

    def _transform(self, df, verbose):
        if verbose:
            print('Determining Exponential Moving Average (period={}) for: "{}"'.format(
                self._period, self._tgt_col))
        df[self._tgt_col] = df[self._src_col].ewm(
            span=self._period, min_periods=self._period, adjust=False, ignore_na=False).mean()
        return df


class SMMA(Indicator):

    def __init__(self, tgt_col, src_col, period=5, **kwargs):
        self._src_col = src_col
        self._tgt_col = tgt_col
        self._period = period
        super().__init__(tgt_col=tgt_col, **kwargs)

    def _transform(self, df, verbose):
        if verbose:
            print('Determining Smoothed Moving Average (period={}) for: "{}"'.format(
                self._period, self._tgt_col))
        df[self._tgt_col] = df[self._src_col].ewm(
            alpha=1.0/self._period).mean().values.flatten()
        return df


class RollingMax(Indicator):

    def __init__(self, tgt_col, src_col, period=5, **kwargs):
        self._tgt_col = tgt_col
        self._src_col = src_col
        self._period = period
        super().__init__(tgt_col=tgt_col, **kwargs)

    def _transform(self, df, verbose):
        if verbose:
            print('Determining Rolling Maximum (period={}) for: "{}"'.format(
                self._period, self._src_col))
        df[self._tgt_col] = df[self._src_col].rolling(self._period).max()
        return df


class RateOfChange(Indicator):

    def __init__(self, tgt_col, src_col, period=5, **kwargs):
        self._tgt_col = tgt_col
        self._src_col = src_col
        self._period = period
        super().__init__(tgt_col=tgt_col, **kwargs)

    def _transform(self, df, verbose):
        if verbose:
            print('Determining Rate of Change (period={}) for: "{}"'.format(
                self._period, self._src_col))

        df['_historical_val'] = df[self._src_col].shift(self._period)
        df[self._tgt_col] = (
            (df[self._src_col] - df['_historical_val']) / df['_historical_val']) * 100
        df.drop('_historical_val', axis=1, inplace=True)

        return df

# max(high - low, abs(high - close[1]), abs(low - close[1]))


class TrueRange(Indicator):

    def __init__(self, tgt_col, high='High', low='Low', close='Close', **kwargs):
        self._tgt_col = tgt_col
        self._high = high
        self._low = low
        self._close = close
        super().__init__(tgt_col=tgt_col, **kwargs)

    def _transform(self, df, verbose):
        if verbose:
            print('Calculating True Range col "{}"'.format(self._tgt_col))

        df['_last_close_'] = df[self._close].shift(1)
        df['_tr_comp_a_'] = df[self._high] - df[self._low]
        df['_tr_comp_b_'] = df[self._high] - df['_last_close_']
        df['_tr_comp_c_'] = df[self._low] - df['_last_close_']
        df[self._tgt_col] = df[['_tr_comp_a_',
                                '_tr_comp_b_', '_tr_comp_c_']].max(axis=1)

        df.drop(['_last_close_', '_tr_comp_a_', '_tr_comp_b_',
                 '_tr_comp_c_'], axis=1, inplace=True)

        return df


class ATR(Indicator):

    def __init__(self, tgt_col, period=5, high='High', low='Low', close='Close', **kwargs):
        self._tgt_col = tgt_col
        self._high = high
        self._low = low
        self._close = close
        self._period = period
        super().__init__(tgt_col=tgt_col, **kwargs)

    def _transform(self, df, verbose):
        if verbose:
            print('Calculating True Range col "{}"'.format(self._tgt_col))

        pipeline = Pipeline([
            TrueRange(self._tgt_col, self._high, self._low, self._close),
            SMMA(self._tgt_col, self._tgt_col, self._period)
        ])

        return pipeline.apply(df)


class HL2(Indicator):

    def __init__(self, tgt_col, high='High', low='Low', **kwargs):
        self._tgt_col = tgt_col
        self._high = high
        self._low = low
        super().__init__(tgt_col=tgt_col, **kwargs)

    def _transform(self, df, verbose):
        if verbose:
            print('Calculating average between High and Low')

        df[self._tgt_col] = (df[self._high] + df[self._low]) / 2
        return df


class SuperTrend(Indicator):

    def __init__(self, tgt_col, period=10, multiplier=3, high='High', low='Low', close='Close', as_offset=True, **kwargs):
        self._tgt_col = tgt_col
        self._period = period
        self._multiplier = multiplier
        self._high = high
        self._low = low
        self._close = close
        self._as_offset = as_offset
        super().__init__(tgt_col=tgt_col, **kwargs)

    def _transform(self, df, verbose):
        if verbose:
            print('Calculating final supertrend bands')

        df = Pipeline([
            HL2('_hl2', high=self._high, low=self._low),
            TrueRange('_tr', high=self._high,
                      low=self._low, close=self._close),
            EMA('_atr', '_tr', period=self._period)
        ]).apply(df)

        df['_basic_lower_band'] = df['_hl2']-(self._multiplier * df['_atr'])
        df['_basic_upper_band'] = df['_hl2']+(self._multiplier * df['_atr'])

        df['_lower_band'] = 0.0
        df['_upper_band'] = 0.0

        for i in range(self._period, len(df)):
            df['_lower_band'].iat[i] = max(df['_basic_lower_band'].iat[i], df['_lower_band'].iat[i-1]
                                           ) if df[self._close].iat[i-1] > df['_lower_band'].iat[i-1] else df['_basic_lower_band'].iat[i]
            df['_upper_band'].iat[i] = min(df['_basic_upper_band'].iat[i], df['_upper_band'].iat[i-1]
                                           ) if df[self._close].iat[i-1] < df['_upper_band'].iat[i-1] else df['_basic_upper_band'].iat[i]

        df['_trend'] = 1
        for i in range(self._period, len(df)):
            if (df['_trend'].iat[i-1] < 0 and df[self._close].iat[i] > df['_upper_band'].iat[i-1]):
                df['_trend'].iat[i] = 1
            elif (df['_trend'].iat[i-1] > 0 and df[self._close].iat[i] < df['_lower_band'].iat[i-1]):
                df['_trend'].iat[i] = -1
            else:
                df['_trend'].iat[i] = df['_trend'].iat[i-1]

        if self._as_offset:
            df[self._tgt_col] = np.where(
                df['_trend'] > 0, df['_hl2']-df['_lower_band'], df['_hl2']-df['_upper_band'])
        else:
            df[self._tgt_col] = np.where(
                df['_trend'] > 0, df['_lower_band'], df['_upper_band'])

        df.drop(['_hl2', '_tr', '_atr', '_basic_lower_band', '_basic_upper_band',
                 '_lower_band', '_upper_band', '_trend'], axis=1, inplace=True)
        return df

    def _indicator(self, df, panel=0):
        orig = self._as_offset
        if not orig:
            self._as_offset = True
            st = self._transform(df, False)[self._tgt_col]
            self._as_offset = orig
        else:
            st = self._get_or_apply(df)[self._tgt_col]
        hl2 = HL2('hl2', high=self._high, low=self._low).apply(df)['hl2']
        ss_upper = np.where(st > 0, hl2 - st, np.nan)
        ss_lower = np.where(st < 0, hl2 - st, np.nan)
        ss_upper[:self._period] = np.nan
        ss_lower[:self._period] = np.nan

        return [
            mpf.make_addplot(ss_upper, panel=panel, color='g',
                             type='line', width=3, alpha=0.5),
            mpf.make_addplot(ss_lower, panel=panel, color='r',
                             type='line', width=3, alpha=0.5)
        ]


class DonchianRibbon(Indicator):

    def __init__(self, tgt_col, period=20, high='High', low='Low', close='Close', debug=False, **kwargs):
        self._tgt_col = tgt_col
        self._period = period
        self._high = high
        self._low = low
        self._close = close
        self._debug = debug
        self._secondary = True

        if period < 10:
            raise ValueError('Period must be 10 or higher.')

        super().__init__(tgt_col=tgt_col, secondary=True, **kwargs)

    def _calc_trend(self, df, p, compare_to_main):
        df['_hh'] = df[self._high].rolling(p).max().shift(1)
        df['_ll'] = df[self._low].rolling(p).min().shift(1)

        df['_trend'] = np.nan
        df.loc[df[self._close] > df['_hh'], '_trend'] = 1
        df.loc[df[self._close] < df['_ll'], '_trend'] = -1
        df['_trend'] = df['_trend'].fillna(method='ffill')

        df['_final_trend'] = 0
        if compare_to_main:
            df.loc[(df['_trend'] > 0) & (df['_main'] > 0), '_final_trend'] = 1
            df.loc[(df['_trend'] < 0) & (df['_main'] < 0), '_final_trend'] = -1
        else:
            df['_final_trend'] = df['_trend']

        trend = df['_final_trend']
        df.drop(['_hh', '_ll', '_trend', '_final_trend'], inplace=True, axis=1)
        return trend

    def _transform(self, df, verbose):
        if verbose:
            print('Calculating Donchian channels for a period of: {}'.format(
                self._period))

        df['_main'] = self._calc_trend(df, self._period, False)
        df['_t1'] = self._calc_trend(df, self._period-1, True)
        df['_t2'] = self._calc_trend(df, self._period-2, True)
        df['_t3'] = self._calc_trend(df, self._period-3, True)
        df['_t4'] = self._calc_trend(df, self._period-4, True)
        df['_t5'] = self._calc_trend(df, self._period-5, True)
        df['_t6'] = self._calc_trend(df, self._period-6, True)
        df['_t7'] = self._calc_trend(df, self._period-7, True)
        df['_t8'] = self._calc_trend(df, self._period-8, True)
        df['_t9'] = self._calc_trend(df, self._period-9, True)

        df[self._tgt_col] = df[['_main', '_t1', '_t2', '_t3', '_t4',
                                '_t5', '_t6', '_t7', '_t8', '_t9']].sum(axis=1).astype('int')

        if not self._debug:
            df.drop(['_main', '_t1', '_t2', '_t3', '_t4', '_t5',
                     '_t6', '_t7', '_t8', '_t9'], inplace=True, axis=1)

        return df

    def _indicator(self, df, panel=0):
        tmp = self._get_or_apply(df)[self._tgt_col]
        pos = np.where(tmp > 0, tmp, 0)
        neg = np.where(tmp <= 0, tmp.abs(), 0)
        return [
            mpf.make_addplot(pos, panel=panel, color='g',
                             type='bar', width=0.75, alpha=self._alpha),
            mpf.make_addplot(neg, panel=panel, color='r',
                             type='bar', width=0.75, alpha=self._alpha)
        ]


class Highest(Indicator):

    def __init__(self, tgt_col, src_col='High', period=5, shift=0, **kwargs):
        self._tgt_col = tgt_col
        self._src_col = src_col
        self._period = period
        self._shift = shift
        super().__init__(tgt_col=tgt_col, **kwargs)

    def _transform(self, df, verbose):
        if verbose:
            print('Calculating highest value for column "{}" over period {}'.format(
                self._src_col, self._period))

        df[self._tgt_col] = df[self._src_col].rolling(
            self._period).max().shift(self._shift)
        return df


class Lowest(Indicator):

    def __init__(self, tgt_col, src_col='Low', period=5, shift=0, **kwargs):
        self._tgt_col = tgt_col
        self._src_col = src_col
        self._period = period
        self._shift = shift
        super().__init__(tgt_col=tgt_col, **kwargs)

    def _transform(self, df, verbose):
        if verbose:
            print('Calculating lowest value for column "{}" over period {}'.format(
                self._src_col, self._period))

        df[self._tgt_col] = df[self._src_col].rolling(
            self._period).min().shift(self._shift)
        return df


class AverageDirectionalIndex(Indicator):

    def __init__(self, tgt_col, period=5, close='Close', high='High', low='Low', **kwargs):
        self._tgt_col = tgt_col
        self._close = close
        self._high = high
        self._low = low
        self._period = period
        super().__init__(tgt_col=tgt_col, **kwargs)

    def _transform(self, df, verbose):
        temp = pd.DataFrame(df[[self._close, self._high, self._low]].copy())

        if verbose:
            print('Calculating average directional index over period {}'.format(
                self._period))

        pipeline = Pipeline([
            pdp.ColByFrameFunc(
                'up', lambda df: df[self._high] - df[self._high].shift(1)),
            pdp.ColByFrameFunc(
                'down', lambda df: df[self._low].shift(1) - df[self._low]),
            pdp.ColByFrameFunc('pdm', lambda df: np.where(
                df['up'] > df['down'], df['up'], 0)),
            pdp.ColByFrameFunc('ndm', lambda df: np.where(
                df['down'] > df['up'], df['down'], 0)),
            SMMA('smma_pdm', 'pdm', self._period),
            SMMA('smma_ndm', 'ndm', self._period),
            ATR('atr', self._period, close=self._close),
            pdp.ColByFrameFunc('pdi', lambda df: (
                100 * df['smma_pdm'] / df['atr'])),
            pdp.ColByFrameFunc('ndi', lambda df: (
                100 * df['smma_ndm'] / df['atr'])),
            pdp.ColByFrameFunc('avg_di', lambda df: (
                ((df['pdi'] - df['ndi']) / (df['pdi'] + df['ndi'])).abs()
            )),
            SMMA('pre_adx', 'avg_di', self._period),
            pdp.ColByFrameFunc('adx', lambda df: (100 * df['pre_adx']))
        ])

        df[self._tgt_col] = pipeline.apply(temp)['adx']

        return df


class DeviationSpread(Indicator):

    def __init__(self, tgt_col, src_col='Close', period=100, **kwargs):
        self._tgt_col = tgt_col
        self._src_col = src_col
        self._period = period
        super().__init__(tgt_col=tgt_col, **kwargs)

    def _transform(self, df, verbose):
        if verbose:
            print('Determining Standard Deviation for: "{}"'.format(self._src_col))

        stddev = df[self._src_col].rolling(self._period).std()
        median = df[self._src_col].rolling(self._period).median()

        df['{}_lowest'.format(self._tgt_col)] = median - 2*stddev
        df['{}_low'.format(self._tgt_col)] = median - stddev
        df['{}_mid'.format(self._tgt_col)] = median
        df['{}_high'.format(self._tgt_col)] = median + stddev
        df['{}_highest'.format(self._tgt_col)] = median + 2*stddev

        return df

    def _indicator(self, df, panel=0):
        tmp = self._get_or_apply(df)
        return [
            mpf.make_addplot(tmp['{}_lowest'.format(self._tgt_col)], panel=panel,
                             color='black', type='line', width=self._width, alpha=0.4),
            mpf.make_addplot(tmp['{}_low'.format(self._tgt_col)], panel=panel,
                             color='blue', type='line', width=self._width, alpha=0.4),
            mpf.make_addplot(tmp['{}_mid'.format(self._tgt_col)], panel=panel,
                             color='purple', type='line', width=self._width, alpha=0.4),
            mpf.make_addplot(tmp['{}_high'.format(self._tgt_col)], panel=panel,
                             color='blue', type='line', width=self._width, alpha=0.4),
            mpf.make_addplot(tmp['{}_highest'.format(self._tgt_col)], panel=panel,
                             color='black', type='line', width=self._width, alpha=0.4)
        ]


class Backtest(Indicator):

    def __init__(self, tgt_col, signal_col, price_col='Close', start_amount=1000000, as_percent=False, **kwargs):
        self._tgt_col = tgt_col
        self._signal_col = signal_col
        self._price_col = price_col
        self._start_amount = start_amount
        self._as_percent = as_percent
        self._secondary = True
        super().__init__(tgt_col=tgt_col, secondary=True, **kwargs)

    def _transform(self, df, verbose):
        if verbose:
            print('Backtesting via Buy/Sell signals from {} with starting amount {}'.format(
                self._signal_col, self._start_amount))

        cur_val = float(self._start_amount)
        free_val = cur_val
        shares_held = 0

        df[self._tgt_col] = 0.0
        for i in range(len(df)):
            if df[self._signal_col].iat[i] == 1.0:
                df[self._tgt_col].iat[i] = free_val
                shares_held = free_val // df[self._price_col].iat[i]
                free_val -= (shares_held * df[self._price_col].iat[i])
            elif df[self._signal_col].iat[i] == -1.0:
                free_val += (shares_held * df[self._price_col].iat[i])
                shares_held = 0
                df[self._tgt_col].iat[i] = free_val
            else:
                df[self._tgt_col].iat[i] = free_val + \
                    (shares_held * df[self._price_col].iat[i])

        if self._as_percent:
            df[self._tgt_col] = (df[self._tgt_col] /
                                 self._start_amount) * 100 - 100.00

        return df

    def _indicator(self, df, panel=0):
        return [mpf.make_addplot(self._get_or_apply(df)[self._tgt_col], panel=panel, color=self._color, type='line', width=self._width, alpha=self._alpha)]


class BuySell(Indicator):

    def __init__(self, tgt_col, src_col, close='Close', high='High', trail_frac=None, sell_eod=False, buy_window=(None, None), sell_window=(None, None), **kwargs):
        self._tgt_col = tgt_col
        self._src_col = src_col
        self._close = close
        self._high = high
        self._trail_frac = trail_frac
        self._sell_eod = sell_eod
        self._buy_window = buy_window
        self._sell_window = sell_window
        super().__init__(tgt_col=tgt_col, **kwargs)

    def _transform(self, df, verbose):
        if verbose:
            print('Converting raw signals ({}) to BuySell timeline events ({}) with trailing stop ({})"'.format(
                self._src_col, self._tgt_col, self._trail_frac))

        in_pos = False
        cur_stop_price = -1.0
        src_val = df[self._src_col]
        df[self._tgt_col] = np.nan

        ts_vals = df.index.to_series()
        eod_times = np.unique(ts_vals.groupby([
            ts_vals.dt.year,
            ts_vals.dt.month,
            ts_vals.dt.day
        ]).transform('max').values)

        eod_ind = np.where(df.index.isin(eod_times), True,
                           False) if self._sell_eod else []

        for i in range(len(df)):
            cur_time = ts_vals.iat[i]
            buy_start = cur_time.replace(hour=int(self._buy_window[0].split(':')[0]), minute=int(
                self._buy_window[0].split(':')[1])) if self._buy_window[0] else cur_time.replace(hour=0, minute=0)
            buy_end = cur_time.replace(hour=int(self._buy_window[1].split(':')[0]), minute=int(
                self._buy_window[1].split(':')[1])) if self._buy_window[0] else cur_time.replace(hour=23, minute=59)

            in_buy_window = buy_start <= cur_time < buy_end

            if src_val.iat[i] > 0 and not in_pos and in_buy_window:
                in_pos = True
                df[self._tgt_col].iat[i] = 1
            elif in_pos:
                stopped_out = df[self._close].iat[i] <= cur_stop_price
                short_sig = src_val.iat[i] < 0
                is_eod = self._sell_eod and eod_ind[i]

                if stopped_out or short_sig or is_eod:
                    in_pos = False
                    cur_stop_price = -1.0
                    df[self._tgt_col].iat[i] = -1
                else:
                    cur_stop_price = max(cur_stop_price, df[self._high].iat[i-1] * (
                        1.0 - self._trail_frac)) if self._trail_frac else -1.0
                    df[self._tgt_col].iat[i] = 0

        return df

    def _indicator(self, df, panel=0):
        temp = self._get_or_apply(df).copy()
        temp['buy'] = np.where(temp[self._tgt_col] > 0,
                               temp[self._close], np.nan)
        temp['sell'] = np.where(temp[self._tgt_col] < 0,
                                temp[self._close], np.nan)

        ret = []
        if len(temp[temp['buy'].notna()]):
            ret.append(mpf.make_addplot(temp['buy'], panel=panel, type='scatter',
                                        markersize=100, marker='^', width=self._width, alpha=self._alpha))
        if len(temp[temp['sell'].notna()]):
            ret.append(mpf.make_addplot(temp['sell'], panel=panel, type='scatter',
                                        markersize=100, marker='v', width=self._width, alpha=self._alpha))

        return ret
