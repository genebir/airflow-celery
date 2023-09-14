from airflow.hooks.base import BaseHook
from plugins.dataframe.cctv import CCTV
import numpy as np
import logging
import pandas as pd


class CCTVHook(BaseHook):

    def __init__(self,
                 path: str,
                 encoding: str = 'utf-8'):
        super().__init__()
        self.cctv = CCTV(path=path,
                         encoding=encoding)
        self.df = self.cctv.df

    def missing_value_repair(self) -> None:
        """
        CCTV_USE 컬럼의 결측 값 처리

        :return:
        """
        cctv_usage = ['산불감시', '방범', '도심공원', '어린이안전', '치수방재', '불법주정차', '자전거보관소', '시설물 관리', '쓰레기 무단투기', '미세먼지']
        self.df['CCTV_USE'] = self.df['CCTV_USE'].apply(lambda x: f"{x.replace(' ', '')}용" if x in cctv_usage else '기타')
        logging.log(level=logging.INFO, msg=f'>>>>>>>>>> Repair Complete : {self.df["CCTV_USE"].unique()}')

    def mk_integrated(self) -> pd.DataFrame:
        """
        데이터 통합을 위한 형태로 데이터프레임을 재구성한다.

        :return: self.df
        """

        self.df = self.df[['ATNRG_NM',
                           'SAFETY_ADDR',
                           'CCTV_USE',
                           'LA',
                           'LO',
                           'PLCST_NM',
                           'PLCST_DEPT_NM',
                           'PLCST_DEPT_CD']]
        logging.log(level=logging.INFO, msg=self.df)
        self.df.columns = ['ATNRG_NM',
                           'SET_ADDR',
                           'USAGE',
                           'LAT',
                           'LON',
                           'PLCST_NM',
                           'PLCST_DEPT_NM',
                           'PLCST_DEPT_CD']
        logging.log(level=logging.INFO, msg=self.df.columns)

        return self.df
