import numpy as np
from datetime import datetime
from pdsando.core.wrappers import PipelineStage


class CastColumns(PipelineStage):
    def __init__(
        self,
        schema,
        strict=False,
        debug=False,
        **kwargs,
    ):
        self._schema = schema
        self._strict = strict
        self._debug = debug
        super().__init__(exmsg="CastColumns failure", desc="ETL Stage")

    def _prec(self, df):
        return True

    def _transform(self, df, verbose):
        for c in df.columns:
            try:
                df[c] = df[c].astype(self._schema.field(c).type.to_pandas_dtype())
            except KeyError as e:
                if self._strict:
                    raise e
        return df


class FillNullsByType(PipelineStage):
    def __init__(self, ignore=None):
        self._ignore = ignore or []
        super().__init__(exmsg="FillNullsByType failure", desc="ETL Stage")

    def _prec(self, df):
        return True

    def _transform(self, df, verbose):
        for c in df.columns:
            if c in self._ignore:
                continue

            fill_val = ""
            if df[c].dtypes.type in (np.int32, np.int64):
                fill_val = 0
            elif df[c].dtypes.type in (np.float32, np.float64):
                fill_val = 0.0
            elif df[c].dtypes.type == np.bool_:
                fill_val = False
            elif df[c].dtypes.type == np.datetime64:
                fill_val = datetime(year=1900, month=1, day=1)
            elif df[c].dtypes.type == np.object_:
                fill_val = ""
            else:
                # Skip unknown types
                continue

            df[c] = df[c].fillna(fill_val)

        return df