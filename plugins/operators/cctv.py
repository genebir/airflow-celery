from airflow.models import BaseOperator
from plugins.hooks.cctv import CCTVHook


class CCTVPreprocessing(BaseOperator):
    def __init__(self,
                 hook: CCTVHook or None = None,
                 path: str or None = None,
                 encoding: str = 'utf-8',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        if hook:
            self.hook = hook
        else:
            self.hook = CCTVHook(path=path,
                                 encoding=encoding)
