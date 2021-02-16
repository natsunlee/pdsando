from pdpipe import PdPipeline, PdPipelineStage


class Pipeline(PdPipeline):
    def __init__(self, *args, **kwargs):
        self._inplace = kwargs.pop('inplace', False)
        super().__init__(*args, **kwargs)

    def apply(self, df, **kwargs):
        if self._inplace:
            return PdPipeline.apply(self, df, **kwargs)
        else:
            return PdPipeline.apply(self, df.copy(), **kwargs)


class PipelineStage(PdPipelineStage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def apply(self, df, **kwargs):
        inplace = kwargs.pop('inplace', True)
        if inplace:
            return PdPipelineStage.apply(self, df, **kwargs)
        else:
            return PdPipelineStage.apply(self, df.copy(), **kwargs)
