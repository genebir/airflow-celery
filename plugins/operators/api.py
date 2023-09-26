from typing import Sequence, Any, Dict

from hooks.api import ApiHook
from airflow.models import BaseOperator


class ApiOperator(BaseOperator):
    template_fields:Sequence[str] = ('service', 'key')

    def __init__(self,
                 hook: ApiHook or None = None,
                 start_index: int = 1,
                 end_index: int = 1,
                 service: str = '',
                 key: str = '',
                 date: str = '',
                 **kwargs):
            super().__init__(**kwargs)
            self.service = service
            self.key = key
            self.start_index = start_index
            self.end_index = end_index
            self.date = date
            self.hook = ApiHook(service=self.service,
                                key=self.key,
                                start_index=self.start_index,
                                end_index=self.end_index,
                                date=date) if not hook else hook

    def execute(self, **context) -> Dict:
        return self.hook.get_data(**context)

