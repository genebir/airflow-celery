import pandas as pd

class CCTV:

    def __init__(self,
                 path: str,
                 encoding: str = 'utf-8',
                 ):
        self.columns = [
            'NO',               # 번호
            'ATNRG_NM',         # 자치구명
            'SAFETY_ADDR',      # 안심주소
            'CCTV_USE',         # CCTV용도
            'LA',               # 위도
            'LO',               # 경도
            'CCTV_QTY',         # CCTV수량
            'UPDATE_DT',        # 수정일시
            'PLCST_NM',         # 경찰서명
            'PLCST_DEPT_NM',    # 경찰서부서명
            'PLCST_DEPT_CD',    # 경찰서부서코드
            'CTPR_PLAGC_NM'     # 시도경찰청명
        ]
        self.path = path
        self.encoding = encoding
        self.df = df = pd.read_csv(self.path,
                                   encoding=self.encoding,
                                   names=self.columns)

    def __str__(self):
        return self.df.__str__()
