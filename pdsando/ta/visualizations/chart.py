import mplfinance as mpf
import numpy as np

from pdsando.ta.pipeline.indicators import Indicator
from pdsando.ta.pipeline.strategies import Strategy


class Chart:
    def __init__(
        self, data, open="Open", high="High", low="Low", close="Close", volume="Volume"
    ):
        self._data = data
        self._indicators = []

        self._open = open
        self._high = high
        self._low = low
        self._close = close
        self._volume = volume
        self._secondary_panel = 1

    def add_indicator(self, ind):
        if not isinstance(ind, Indicator) or isinstance(ind, Strategy):
            raise TypeError("Must be of Indicator type")

        p = 0
        if ind.secondary:
            p = self._secondary_panel
            self._secondary_panel += 1

        for x in ind._indicator(self._data, p):
            self._indicators.append(x)

    def draw(self, session_breaks=True, figsize=(35, 15), savefig=None):
        vlines = []
        if session_breaks:
            ts_vals = self._data.index.to_series()
            min_times = (
                ts_vals.groupby([ts_vals.dt.year, ts_vals.dt.month, ts_vals.dt.day])
                .transform("min")
                .values
            )
            vlines = list(self._data[(self._data.index == min_times)].index)

        # Generate indicator data
        apd = []
        for x in self._indicators:
            try:
                apd.extend(x._indicator(self._data))
            except AttributeError as ae:
                print(ae)
            except NotImplementedError as nie:
                print(nie)

        # Draw plot
        if savefig:
            mpf.plot(
                self._data,
                type="candlestick",
                style="yahoo",
                figsize=figsize,
                addplot=apd,
                savefig=savefig,
                tight_layout=True,
                vlines=dict(vlines=vlines, alpha=0.35, linewidths=1),
            )
        else:
            mpf.plot(
                self._data,
                type="candlestick",
                style="yahoo",
                figsize=figsize,
                addplot=apd,
                vlines=dict(vlines=vlines, alpha=0.35, linewidths=1),
            )

    def draw_pipeline(
        self, pipeline, session_breaks=True, figsize=(35, 15), savefig=None
    ):
        df = pipeline.apply(self._data)
        vlines = []
        colors = []
        if session_breaks:
            ts_vals = self._data.index.to_series()
            min_times = (
                ts_vals.groupby([ts_vals.dt.year, ts_vals.dt.month, ts_vals.dt.day])
                .transform("min")
                .values
            )
            vlines = list(self._data[(self._data.index == min_times)].index)
            colors.extend(["b"] * len(vlines))

        # Generate indicator data
        apd = []
        for x in [
            x for x in pipeline if isinstance(x, Indicator) or isinstance(x, Strategy)
        ]:
            if x.vlines:
                vl = x._indicator(df, 0)
                vlines.extend(vl)
                colors.extend([x.color] * len(vl))
            else:
                p = 0
                if x.secondary:
                    p = self._secondary_panel
                    self._secondary_panel += 1
                try:
                    apd.extend(x._indicator(df, p))
                except AttributeError as ae:
                    print(ae)
                    if x.secondary:
                        self._secondary_pane -= 1
                except NotImplementedError as nie:
                    print(nie)
                    if x.secondary:
                        self._secondary_pane -= 1

        # Draw plot
        if savefig:
            mpf.plot(
                df,
                type="candlestick",
                style="yahoo",
                figsize=figsize,
                addplot=apd,
                savefig=savefig,
                tight_layout=True,
                vlines=dict(vlines=vlines, alpha=0.35, linewidths=1, colors=colors),
            )
        else:
            mpf.plot(
                df,
                type="candlestick",
                style="yahoo",
                figsize=figsize,
                addplot=apd,
                vlines=dict(vlines=vlines, alpha=0.35, linewidths=1, colors=colors),
            )
