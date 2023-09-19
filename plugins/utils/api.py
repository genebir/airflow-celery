from enum import Enum
from airflow.models.variable import Variable

class Api(Enum):
    SUBWAY_API__URL = 'http://swopenapi.seoul.go.kr/api/subway/'
    OPEN_API_URL = 'http://openapi.seoul.go.kr:8088/'
    AUTH_KEY = Variable.get('api_auth_key')
    # 지하철 실시간 열차 위치정보
    REALTIME_POSITION = 'realtimePosition/'
