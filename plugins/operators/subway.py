from airflow.models.baseoperator import BaseOperator
from hooks.subway import SubwayHook
class SubwayOperator(BaseOperator):
    def __init__(self,
                 subway_name: str = None,
                 hook = None,
                 *args,
                 **kwargs):
        super().__init__(*args,
                         **kwargs)
        self.hook = hook if hook else SubwayHook(subway_name=subway_name)

    def execute(self, context):
        self.hook.transform()

