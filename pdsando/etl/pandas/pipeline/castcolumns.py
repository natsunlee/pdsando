from pdsando.core.wrappers import PipelineStage


class CastColumns(PipelineStage):
    def __init__(
        self,
        schema,
        debug=False,
        **kwargs,
    ):
        self._schema = schema
        self._debug = debug
        super().__init__(exmsg="CastColumns failure", desc="ETL Stage")

    def _prec(self, df):
        return True

    def _transform(self, df, verbose):
        ret_df = df
        for c in ret_df.columns:
            ret_df[c] = ret_df[c].astype(self._schema.field(c).type.to_pandas_dtype())
        return ret_df
