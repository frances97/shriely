# -*- coding: utf-8 -*-
from __future__ import division
import  cx_Oracle   as  ora
import pandas as pd
import datetime
from   settings   import  DATABASES
import numpy as np
from math import isnan
from scipy import stats
import os
import time
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
def tic():
    globals()['tt'] = time.clock()
    
def toc():
    print '\nElapsed time: %.8f seconds\n' % (time.clock()-globals()['tt'])

tic()

'''
@attention:    用于从数据中心获取 个股诊断 数据
'''
class   Stock_Diagnosis_Data(object):
    
    def __init__(self):
        #self._conn_Mysql_stkDiag = MySQLdb.connect(host=DATABASES["Stock_Diagnosis"].get("host"), user=DATABASES["Stock_Diagnosis"].get("user"), passwd=DATABASES["Stock_Diagnosis"].get("passwd"), db=DATABASES["Stock_Diagnosis"].get("db"), charset="utf8")
        #self._engine_Mysql_stkDiag = create_engine(DATABASES["Stock_Diagnosis"].get("engine") + "://" + DATABASES["Stock_Diagnosis"].get("user") + ":" + DATABASES["Stock_Diagnosis"].get("passwd") + "@" + DATABASES["Stock_Diagnosis"].get("host") + "/" + DATABASES["Stock_Diagnosis"].get("db") + "?charset=utf8")
        #self._conn_Mysql_hq = MySQLdb.connect(host=DATABASES["HqData"].get("host"), user=DATABASES["HqData"].get("user"), passwd=DATABASES["HqData"].get("passwd"), db=DATABASES["HqData"].get("db"), charset="utf8")
        #self._engine_Mysql_hq = create_engine(DATABASES["HqData"].get("engine") + "://" + DATABASES["HqData"].get("user") + ":" + DATABASES["HqData"].get("passwd") + "@" + DATABASES["HqData"].get("host") + "/" + DATABASES["HqData"].get("db") + "?charset=utf8")
        #self._dsn = ora.makedsn(DATABASES["aliyun"].get("host", "121.43.68.222"),DATABASES["aliyun"].get("port","15210"), DATABASES["aliyun"].get("sid", "upwhdb"))
        #self._conn_Mysql_IndexDB = MySQLdb.connect(host=DATABASES["IndexDB"].get("host"), user=DATABASES["IndexDB"].get("user"), passwd=DATABASES["IndexDB"].get("passwd"), db=DATABASES["IndexDB"].get("db"), charset="utf8")
        self._dsn = ora.makedsn(host= "121.43.68.222", port="15210", sid= "upwhdb") #阿里云的连接设置
        self._conn_Ora = ora.connect(DATABASES["aliyun"].get("user"), DATABASES["aliyun"].get("passwd"),self._dsn)
        self._dbcenter_dsn=ora.makedsn(host= "172.16.8.20", port="1521", sid= "dbcenter") #数据中心本地的数据库 upapp
        self._dbcenter_conn_Ora= ora.connect(DATABASES["datacenter"].get("user"), DATABASES["datacenter"].get("passwd"),self._dbcenter_dsn)
        
        self._stkpool_uni = []
        self._stkpool = []
        self._stkpool_num = 0
        self._NewTradeDate = 0#大盘最新交易日期
        self._stk_indu=pd.DataFrame()#用于存储股票代码和行业的对应关系
        self._fin_data=pd.DataFrame() #用于存储净利润,每股收益,每股经营活动产生的额现金流量净额,每股净资产
        self._fin_data_new=pd.DataFrame() #用于存储公司盈利能力(销售净利率、销售毛利率、净资产收益率)、成长能力(营业收入同比增长率、营业利润同比增长率、净利润同比增长率)、资产质量及负债(总资产周转率、应收账款周转率、资产负债率)
        self._cash_flow=pd.DataFrame()#用来存最新一期所有A股的最新一期的现金流数据
        self._insti_rate=pd.DataFrame() #用来存储机构评级的数据
        self._pre_eps=pd.DataFrame() #用来存储所有A股的预测eps
        self._pe_close=pd.DataFrame() #存储所有A股的pe和close
    '''
    @function: 取 股票池股票 数据
    '''
    def Get_StkBaseInfo(self):
        '''
        strsql = "SELECT code FROM stock_baseinfo"
        dfStkInfo = pd.read_sql(strsql, self._conn_Mysql_hq)
        for stkcode in dfStkInfo["code"]:
            self._stkpool.append(stkcode)
        self._stkpool_num = len(self._stkpool)
        '''
        strsql = "select a.stk_code,a.STK_UNI_CODE from upcenter.STK_BASIC_INFO a where a.isvalid=1 and a.LIST_STA_PAR in ('1','4') and a.SEC_MAR_PAR in('1','2') and a.STK_TYPE_PAR=1 and a.LIST_DATE<trunc(sysdate)"
        dfOra = pd.read_sql(strsql,self._dbcenter_conn_Ora)
        self._stkpool=list(dfOra['STK_CODE'])
        self._stkpool_num = len(self._stkpool)
        
        strsql = "SELECT * from (select m.end_date from upcenter.PUB_EXCH_CALE m where m.isvalid=1 and m.SEC_MAR_PAR=2 and m.IS_TRADE_DATE=1 and m.end_date <trunc(sysdate) ORDER BY m.END_DATE DESC) WHERE ROWNUM < 2"
        dfOra = pd.read_sql(strsql,self._dbcenter_conn_Ora)
        if  not dfOra["END_DATE"].empty:
            if  dfOra["END_DATE"][0] != None:
                self._NewTradeDate = dfOra["END_DATE"][0]
        #股票代码和行业的关系       
        strsql="select a.stk_code,c.INDU_NAME from upcenter.STK_BASIC_INFO a,upcenter.PUB_COM_INDU_RELA b,upcenter.PUB_INDU_CODE c where a.COM_UNI_CODE=b.COM_UNI_CODE and b.INDU_UNI_CODE=c.INDU_UNI_CODE and b.INDU_SYS_CODE=16 and c.INDU_SYS_PAR=16 and a.STK_TYPE_PAR=1 and a.SEC_MAR_PAR in ('1','2') and a.LIST_SECT_PAR in ('1','2','3') and a.LIST_STA_PAR in ('1','4') and a.LIST_DATE<trunc(sysdate)"
        self._stk_indu=pd.read_sql(strsql, self._dbcenter_conn_Ora)
        #所有股票最新一期的财务数据
        strsql="select STK_CODE,END_DATE,INC_I,BEPS,PS_OCF,BPS from (select b.STK_CODE,a.END_DATE,a.INC_I,a.BEPS,a.PS_OCF,a.BPS,ROW_NUMBER() OVER(partition by b.stk_code ORDER BY a.END_DATE desc) as rk from upcenter.FIN_IDX_ANA a,upcenter.STK_BASIC_INFO b where b.STK_TYPE_PAR=1 and SEC_MAR_PAR in ('1','2') and LIST_SECT_PAR in ('1','2','3') and a.com_uni_code=b.com_uni_code and b.LIST_STA_PAR in ('1','4') and b.LIST_DATE<trunc(sysdate) order by a.END_DATE DESC) where rk=1"
        #strsql="select b.STK_CODE,a.END_DATE,a.INC_I,a.BEPS,a.PS_OCF,a.BPS from upcenter.FIN_IDX_ANA a,upcenter.STK_BASIC_INFO b where b.STK_TYPE_PAR=1 and SEC_MAR_PAR in ('1','2') and LIST_SECT_PAR in ('1','2','3') and a.com_uni_code=b.com_uni_code and b.LIST_STA_PAR in ('1','4') and b.LIST_DATE<trunc(sysdate) and extract(year from a.end_date)>extract(year from sysdate)-3  order by b.Stk_Code desc, a.END_DATE desc"
        self._fin_data=pd.read_sql(strsql,self._conn_Ora)
        #取出最近两期的公司盈利能力(销售净利率、销售毛利率、净资产收益率)、成长能力(营业收入同比增长率、营业利润同比增长率、净利润同比增长率)、资产质量及负债(总资产周转率、应收账款周转率、资产负债率)
        strsql="select a.end_date,b.stk_code,a.SAL_NPR,a.SAL_GIR,a.ROEA,a.OR_YOY,a.OP_YOY,a.NP_YOY,a.TA_RATE,a.AP_RATE,a.BAL_P,a.BAL_O from upcenter.FIN_IDX_ANA a,upcenter.STK_BASIC_INFO b where a.com_uni_code=b.com_uni_code  and extract (year from a.end_date) >=extract(year from trunc(sysdate))-2 and b.isvalid=1 and b.LIST_STA_PAR in ('1','4') and b.SEC_MAR_PAR in('1','2') and b.STK_TYPE_PAR=1 and b.LIST_DATE<trunc(sysdate) order by stk_code desc,a.end_date desc"
        self._fin_data_new=pd.read_sql(strsql,self._conn_Ora)
        self._fin_data_new['debt_to_ability']=self._fin_data_new['BAL_P']/self._fin_data_new['BAL_O'] 
        
        strsql="select end_date,stk_code,CS_10000,CS_20000,CS_30000 FROM (select a.end_date,b.stk_code,Cast(a.CS_10000/power(10,8) as decimal(18,4)) as CS_10000 ,Cast(a.CS_20000/power(10,8) as decimal(18,4)) as CS_20000,Cast(a.CS_30000/power(10,8) as decimal(18,4)) as CS_30000,row_number() over (partition by b.stk_code order by a.end_date desc) as rk from upcenter.FIN_CASH_SHORT a,upcenter.STK_BASIC_INFO b where a.COM_UNI_CODE=b.COM_UNI_CODE  and b.isvalid=1 and b.LIST_STA_PAR in ('1','4') and b.SEC_MAR_PAR in('1','2') and b.STK_TYPE_PAR=1 and b.LIST_DATE<trunc(sysdate) order by a.end_date desc) WHERE RK=1"
        self._cash_flow=pd.read_sql(strsql,self._conn_Ora)
        
        strsql="SELECT C.STK_CODE,a.DECL_DATE,A.RES_RATE_PAR FROM UPCENTER.RES_STK_FORE_PRICE A , UPCENTER.PUB_ORG_INFO B,UPCENTER. STK_BASIC_INFO C WHERE A.ORG_UNI_CODE = B.ORG_UNI_CODE AND A.STK_UNI_CODE=C.STK_UNI_CODE AND DECL_DATE > TRUNC(SYSDATE)-90 AND A.ISVALID = 1 AND B.ISVALID = 1 AND C.STK_TYPE_PAR=1 AND C.SEC_MAR_PAR IN ('1','2') AND C.LIST_SECT_PAR IN ('1','2','3') AND C.LIST_STA_PAR IN ('1','4') ORDER BY C.STK_CODE,A.DECL_DATE DESC"
        self._insti_rate=pd.read_sql(strsql,self._conn_Ora)
        
        strsql="select stk_code,end_date,subj_avg from (select a.END_DATE,b.stk_code,FORE_YEAR,SUBJ_AVG,ROW_NUMBER() OVER (partition by b.stk_code ORDER BY a.END_DATE desc) as rk from upcenter.RES_COM_PROFIT_FORE a,upcenter.STK_BASIC_INFO b where a.isvalid=1 and a.SEC_UNI_CODE=b.STK_UNI_CODE and a.SUBJ_CODE=14 and FORE_YEAR=to_number(to_char(sysdate,'yyyy')) and STAT_RANGE_PAR=4 order by end_date desc,FORE_YEAR desc) where rk=1"
        self._pre_eps=pd.read_sql(strsql,self._conn_Ora)
        
        strsql="SELECT stk_code,trade_date,CLOSE_PRICE,STK_PER_TTM FROM (select a.stk_code,b.TRADE_DATE,b.CLOSE_PRICE,b.STK_PER_TTM,row_number() over (partition by a.stk_code ORDER BY b.TRADE_DATE DESC ) AS RK from upcenter.STK_BASIC_INFO a,upcenter.STK_BASIC_PRICE_MID b where a.STK_UNI_CODE=b.STK_UNI_CODE and a.isvalid=1 and b.end_date=b.trade_date and a.STK_TYPE_PAR=1 and a.SEC_MAR_PAR in ('1','2')  and a.LIST_SECT_PAR in ('1','2','3') and a.LIST_STA_PAR in ('1','4') order by b.Trade_Date desc) WHERE RK=1"
        self._pe_close=pd.read_sql(strsql,self._conn_Ora)
    '''
    @function 给出净利润,每股收益,每股经营活动产生的额现金流量净额,每股净资产的值和排名
    '''    
    def Get_Value_Rank(self,stkcode):
#        tradeyear = int(self._NewTradeDate.strftime("%Y"))
#        trademonth = int(self._NewTradeDate.strftime("%m"))
#        tradeday = int(self._NewTradeDate.strftime("%d"))
#        today = datetime.date.today()
#        yesterday = today - datetime.timedelta(days=1)
#        if  (tradeyear == yesterday.year and trademonth == yesterday.month and tradeday == yesterday.day):
#            pass
#        else:
#            return#不是的话就不计算，防止星期天重复计算上周五的数据
        #得到个股所属的行业
        location=list(self._stk_indu['STK_CODE']).index(stkcode)
        indu_name=self._stk_indu.iloc[location,1] #得到个股所属行业
        
        #得到个股所在行业的成分股
        stk_indu_consi=self._stk_indu[self._stk_indu['INDU_NAME']==indu_name]
        stk_code=list(stk_indu_consi['STK_CODE'])#成分股代码
        fin_data=self._fin_data[self._fin_data['STK_CODE'].isin(stk_code)]
        #return fin_data
#        descri=""
        #成分股最新季度的数据 #净利润 #每股收益 #每股经营活动产生的现金流量净额 #每股净资产#and extract(month from a.end_date) = 12 
#        for code in stk_code:
#            strsql="select STK_CODE,END_DATE,INC_I,BEPS,PS_OCF,BPS from (select b.STK_CODE,a.END_DATE,a.INC_I,a.BEPS,a.PS_OCF,a.BPS,ROW_NUMBER() OVER (ORDER BY a.END_DATE desc) as rk from upcenter.FIN_IDX_ANA a,upcenter.STK_BASIC_INFO b where b.STK_TYPE_PAR=1 and SEC_MAR_PAR in ('1','2') and LIST_SECT_PAR in ('1','2','3') and a.com_uni_code=b.com_uni_code and b.LIST_STA_PAR in ('1','4') and b.LIST_DATE<trunc(sysdate)  and STK_CODE="+"'"+code+"'"+" order by a.END_DATE DESC) where rk=1"
#            stk_fin_data=pd.read_sql(strsql,self._conn_Ora)
#            fin_data=fin_data.append(stk_fin_data)
        #找到行业中的最小日期
        min_date=min(fin_data['END_DATE'])
        stock_code=list(fin_data[fin_data['END_DATE']!=min_date]['STK_CODE'])#找到行业中最新财务时间不是最小日期的股票代码
        date=str(min_date)[0:11].replace("-","")
        #将最新日期不是最小值的股票 替换为最小日期的值
        if len(stock_code)!=0:
            for code in stock_code:
                strsql="select b.STK_CODE,a.END_DATE,a.INC_I,a.BEPS,a.PS_OCF,a.BPS from upcenter.FIN_IDX_ANA a,upcenter.STK_BASIC_INFO b where b.STK_TYPE_PAR=1 and SEC_MAR_PAR in ('1','2') and LIST_SECT_PAR in ('1','2','3') and a.com_uni_code=b.com_uni_code and b.LIST_STA_PAR in ('1','4') and b.LIST_DATE<trunc(sysdate)  and STK_CODE="+"'"+code+"'"+"  and a.END_DATE=TO_DATE("+"'"+date+"'"+",'yyyy-mm-dd') order by a.END_DATE DESC"
                fin_data_complem=pd.read_sql(strsql,self._conn_Ora)
                fin_data=pd.concat([fin_data[fin_data['END_DATE']==min_date],fin_data_complem])
                
        #找到该只股票的四个维度的值以及行业排名
        #stkcode="002839"
        num=fin_data.shape[0] #行业中的个股数
        if stkcode in list(fin_data['STK_CODE']):
            location=list(fin_data['STK_CODE']).index(stkcode)
            stk_inc_i=fin_data.iloc[location,2]
            if stk_inc_i!=None:
                stk_inc_i=round(fin_data.iloc[location,2]/(10**8),4)#/(10**8) #单位为亿元 净利润
            else:
                stk_inc_i="--"
                
            stk_beps=fin_data.iloc[location,3]
            if stk_beps!=None:
                stk_beps=round(fin_data.iloc[location,3],4) #每股收益
            else:
                stk_beps="--"
                
            stk_ps_ocf=fin_data.iloc[location,4]
            if stk_ps_ocf!=None:
                stk_ps_ocf=round(fin_data.iloc[location,4],4) #每股经营活动产生的现金流量净额
            else:
                stk_ps_ocf="--"
                
            stk_bps=fin_data.iloc[location,5]
            
            if stk_bps!=None:
                stk_bps=round(fin_data.iloc[location,5],4) #每股净资产
            else:
                stk_bps="--"
            
            if stk_inc_i!="--":
                #找到四个维度 该只股票在行业中的排名
                fin_data=fin_data.sort_values(by="INC_I",ascending=False)
                rank_inc_i=list(fin_data['STK_CODE']).index(stkcode)+1
            else:
                rank_inc_i="--"
                
            if stk_beps!="--":
                fin_data=fin_data.sort_values(by="BEPS",ascending=False)
                rank_beps=list(fin_data['STK_CODE']).index(stkcode)+1
            else:
                rank_beps="--"
            
            if stk_ps_ocf!="--":
                fin_data=fin_data.sort_values(by="PS_OCF",ascending=False)
                rank_ps_ocf=list(fin_data['STK_CODE']).index(stkcode)+1
            else:
                rank_ps_ocf="--"
            
            if stk_bps!="--":
                fin_data=fin_data.sort_values(by="BPS",ascending=False)
                rank_bps=list(fin_data['STK_CODE']).index(stkcode)+1
            else:
                rank_bps="--"
    
            descri_year=date[0:4]
            if date[4:6]=='03':
                descri_quar='第一季度'
            elif date[4:6]=='06':
                descri_quar='第二季度'
            elif date[4:6]=='09':
                descri_quar='第三季度'
            elif date[4:6]=='12':
                descri_quar='第四季度'
            #判断公司品质优秀与否
            length=fin_data.shape[0]
            if rank_beps!="--":
                if np.floor(rank_beps/length*100)<=25:
                    descri_quali='综合以上数据分析,公司质地优秀.'
                elif np.floor(rank_beps/length*100)>25 and np.floor(rank_beps/length*100)<=50:
                    descri_quali='综合以上数据分析,公司质地良好.'
                elif np.floor(rank_beps/length*100)>50 and np.floor(rank_beps/length*100)<=75:
                    descri_quali='综合以上数据分析,公司质地一般.'
                elif np.floor(rank_beps/length*100)>75 and np.floor(rank_beps/length*100)<=100:
                    descri_quali='综合以上数据分析,公司质地较差.'
            elif rank_inc_i!="--":
                if np.floor(rank_inc_i/length*100)<=25:
                    descri_quali='综合以上数据分析,公司质地优秀.'
                elif np.floor(rank_inc_i/length*100)>25 and np.floor(rank_inc_i/length*100)<=50:
                    descri_quali='综合以上数据分析,公司质地良好.'
                elif np.floor(rank_inc_i/length*100)>50 and np.floor(rank_inc_i/length*100)<=75:
                    descri_quali='综合以上数据分析,公司质地一般.'
                elif np.floor(rank_inc_i/length*100)>75 and np.floor(rank_inc_i/length*100)<=100:
                    descri_quali='综合以上数据分析,公司质地较差.'
            else:
                descri_quali='综合以上数据分析,公司质地--.'
                    
            descri="所属行业为"+indu_name+","+descri_year+"年"+descri_quar+"净利润为"+str(stk_inc_i)+"亿元,"+"排名行业第"+str(rank_inc_i)+";每股收益为"+str(stk_beps)+"元,"+"排名行业第"+str(rank_beps)+";"+"每股经营活动产生的现金流量净额为"+str(stk_ps_ocf)+",排名行业第"+str(rank_ps_ocf)+";每股净资产为"+str(stk_bps)+",排名行业第"+str(rank_bps)+"."+descri_quali
            # return descri
        else:
            descri="当前无可用数据"
        return descri,rank_inc_i,rank_beps,rank_ps_ocf,rank_bps,num,descri_quali
        
    '''
    @function 公司盈利能力(销售净利率、销售毛利率、净资产收益率)、成长能力(营业收入同比增长率、营业利润同比增长率、净利润同比增长率)、资产质量及负债(总资产周转率、应收账款周转率、资产负债率)、现金流(经营活动现金净额、投资活动现金净额、筹资活动现金净额)
    '''
    def Company_Manage(self,stkcode):

        #strsql="select a.end_date,a.SAL_NPR,a.SAL_GIR,a.ROEA from upcenter.FIN_IDX_ANA a,upcenter.STK_BASIC_INFO b where a.com_uni_code=b.com_uni_code and  b.stk_code="+"'"+stkcode+"'"+" and a.end_date >= to_date('20110331','yyyy-mm-dd') order by a.end_date desc"
       
        #时间和输出对应
#        date_new=filter(lambda x:x.year==max(dfOra['END_DATE']).year,dfOra['END_DATE'])
#        date_old=filter(lambda x:x.month==12,dfOra['END_DATE'])
#        date=date_new+date_old
#        date=list(set(date))
        '''
        盈利能力(销售净利率、销售毛利率、净资产收益率)
        '''
#        earning_ability=dfOra[dfOra['END_DATE'].isin(date)] #找到个股的盈利能力 分别对应 销售净利率、销售毛利率、净资产收益率
        #比较销售净利率、销售毛利率、净资产收益率 本期和上期的差别
        dfOra=self._fin_data_new[self._fin_data_new['STK_CODE']==stkcode]
        month=list(dfOra['END_DATE'])[0].month
        fin_data_two=dfOra[dfOra['END_DATE'].isin(filter(lambda x:x.month==month,dfOra['END_DATE']))]
        if fin_data_two.shape[0]>1:
            fin_data_two=fin_data_two.iloc[[0,1],:]
            if not isnan(list(fin_data_two['SAL_NPR'])[0]) and not isnan(list(fin_data_two['SAL_NPR'])[1]):
                if list(fin_data_two['SAL_NPR'])[0]-list(fin_data_two['SAL_NPR'])[1]>0:
                    descri_SAL_NPR='本期销售净利率大于上期销售净利率'
                elif list(fin_data_two['SAL_NPR'])[0]-list(fin_data_two['SAL_NPR'])[1]==0:
                    descri_SAL_NPR='本期销售净利率等于上期销售净利率'
                elif list(fin_data_two['SAL_NPR'])[0]-list(fin_data_two['SAL_NPR'])[1]<0:
                    descri_SAL_NPR='本期销售净利率小于上期销售净利率'
            else:
                descri_SAL_NPR='本期销售净利率与上期销售净利率无法比较'
                
             #这里分了两种为空的情况：第一种：最新一期公布数据（也就是2016-12-31），很多公司只出了业绩快报，但还没出财务报表，所以无法计算出销售毛利率；第二种：销售毛利率一般是只针对非金融企业才计算的，金融类企业一般不会计算这个指标。   
            if not list(fin_data_two['SAL_GIR'])[0] is None and not list(fin_data_two['SAL_GIR'])[1] is None:
                if not isnan(list(fin_data_two['SAL_GIR'])[0]) and not isnan(list(fin_data_two['SAL_GIR'])[1]):
                    if list(fin_data_two['SAL_GIR'])[0]-list(fin_data_two['SAL_GIR'])[1]>0:
                        descri_SAL_GIR='本期销售毛利率大于上期销售毛利率'
                    elif list(fin_data_two['SAL_GIR'])[0]-list(fin_data_two['SAL_GIR'])[1]==0:
                        descri_SAL_GIR='本期销售毛利率等于上期销售毛利率'
                    elif list(fin_data_two['SAL_GIR'])[0]-list(fin_data_two['SAL_GIR'])[1]<0:
                        descri_SAL_GIR='本期销售毛利率小于上期销售毛利率'
                else:
                    descri_SAL_GIR='本期销售毛利率与上期销售毛利率无法比较'
            else:
                descri_SAL_GIR='本期销售毛利率与上期销售毛利率无法比较'
            
            if not isnan(list(fin_data_two['ROEA'])[0]) and not isnan(list(fin_data_two['ROEA'])[1]):
                if list(fin_data_two['ROEA'])[0]-list(fin_data_two['ROEA'])[1]>0:
                    descri_ROEA='本期净资产收益率大于上期净资产收益率'
                elif list(fin_data_two['ROEA'])[0]-list(fin_data_two['ROEA'])[1]==0:
                    descri_ROEA='本期净资产收益率等于上期净资产收益率'
                elif list(fin_data_two['ROEA'])[0]-list(fin_data_two['ROEA'])[1]<0:
                    descri_ROEA='本期净资产收益率小于上期净资产收益率'
            else:
                descri_ROEA='本期净资产收益率与上期净资产收益率无法比较'
            '''
            成长能力(营业收入同比增长率、营业利润同比增长率、净利润同比增长率) 值都乘以了100
            '''
            if not isnan(list(fin_data_two['OR_YOY'])[0]) and not isnan(list(fin_data_two['OR_YOY'])[1]):
                if  list(fin_data_two['OR_YOY'])[0]-list(fin_data_two['OR_YOY'])[1]>0:
                    descri_OR_YOY='本期营业收入同比增长率大于上期营业收入同比增长率'
                elif list(fin_data_two['OR_YOY'])[0]-list(fin_data_two['OR_YOY'])[1]==0:
                    descri_OR_YOY='本期营业收入同比增长率等于上期营业收入同比增长率'
                elif list(fin_data_two['OR_YOY'])[0]-list(fin_data_two['OR_YOY'])[1]<0:
                     descri_OR_YOY='本期营业收入同比增长率小于上期营业收入同比增长率'
            else:
                descri_OR_YOY='本期营业收入同比增长率与上期营业收入同比增长率无法比较'
            
            if not isnan(list(fin_data_two['OP_YOY'])[0]) and not isnan(list(fin_data_two['OP_YOY'])[1]):
                if  list(fin_data_two['OP_YOY'])[0]-list(fin_data_two['OP_YOY'])[1]>0:
                    descri_OP_YOY='本期营业利润同比增长率大于上期营业利润同比增长率'
                elif list(fin_data_two['OP_YOY'])[0]-list(fin_data_two['OP_YOY'])[1]==0:
                    descri_OP_YOY='本期营业利润同比增长率等于上期营业利润同比增长率'
                elif list(fin_data_two['OP_YOY'])[0]-list(fin_data_two['OP_YOY'])[1]<0:
                     descri_OP_YOY='本期营业利润同比增长率小于上期营业利润同比增长率'
            else:
                descri_OP_YOY='本期营业利润同比增长率与上期营业利润同比增长率无法比较'
                
            if not isnan(list(fin_data_two['NP_YOY'])[0]) and not isnan(list(fin_data_two['NP_YOY'])[1]):
                if  list(fin_data_two['NP_YOY'])[0]-list(fin_data_two['NP_YOY'])[1]>0:
                    descri_NP_YOY='本期净利润同比增长率大于上期净利润同比增长率'
                elif list(fin_data_two['NP_YOY'])[0]-list(fin_data_two['NP_YOY'])[1]==0:
                    descri_NP_YOY='本期净利润同比增长率等于上期净利润同比增长率'
                elif list(fin_data_two['NP_YOY'])[0]-list(fin_data_two['NP_YOY'])[1]<0:
                     descri_NP_YOY='本期净利润同比增长率小于上期净利润同比增长率'
            else:
                descri_NP_YOY='本期净利润同比增长率与上期净利润同比增长率无法比较'
            '''
            资产质量及负债(总资产周转率、应收账款周转率、资产负债率) 
            '''    
                
            if not isnan(list(fin_data_two['TA_RATE'])[0]) and not isnan(list(fin_data_two['TA_RATE'])[1]):
                if  list(fin_data_two['TA_RATE'])[0]-list(fin_data_two['TA_RATE'])[1]>0:
                    descri_TA_RATE='本期总资产周转率大于上期总资产周转率'
                elif list(fin_data_two['TA_RATE'])[0]-list(fin_data_two['TA_RATE'])[1]==0:
                    descri_TA_RATE='本期总资产周转率等于上期总资产周转率'
                elif list(fin_data_two['TA_RATE'])[0]-list(fin_data_two['TA_RATE'])[1]<0:
                     descri_TA_RATE='本期总资产周转率小于上期总资产周转率'
            else:
                descri_TA_RATE='本期总资产周转率与上期总资产周转率无法比较'
                    
                
            if not list(fin_data_two['AP_RATE'])[0] is None and list(fin_data_two['AP_RATE'])[1] is None:
                if not isnan(list(fin_data_two['AP_RATE'])[0]) and not isnan(list(fin_data_two['AP_RATE'])[1]):
                    if  list(fin_data_two['AP_RATE'])[0]-list(fin_data_two['AP_RATE'])[1]>0:
                       descri_AP_RATE='本期应收账款周转率大于上期应收账款周转率'
                    elif list(fin_data_two['AP_RATE'])[0]-list(fin_data_two['AP_RATE'])[1]==0:
                       descri_AP_RATE='本期应收账款周转率等于上期应收账款周转率'
                    elif list(fin_data_two['AP_RATE'])[0]-list(fin_data_two['AP_RATE'])[1]<0:
                        descri_AP_RATE='本期应收账款周转率小于上期应收账款周转率'
                else:
                   descri_AP_RATE='本期应收账款周转率与上期应收账款周转率无法比较'
            else:
               descri_AP_RATE='本期总资产周转率与上期总资产周转率无法比较' 
                
            if not isnan(list(fin_data_two['debt_to_ability'])[0]) and not isnan(list(fin_data_two['debt_to_ability'])[1]):
                if  list(fin_data_two['debt_to_ability'])[0]-list(fin_data_two['debt_to_ability'])[1]>0:
                    descri_debt_to_ability='本期资产负债率大于上期资产负债率'
                elif list(fin_data_two['debt_to_ability'])[0]-list(fin_data_two['debt_to_ability'])[1]==0:
                    descri_debt_to_ability='本期资产负债率等于上期资产负债率'
                elif list(fin_data_two['debt_to_ability'])[0]-list(fin_data_two['debt_to_ability'])[1]<0:
                     descri_debt_to_ability='本期资产负债率小于上期资产负债率'
            else:
                descri_debt_to_ability='本期资产负债率与上期资产负债率无法比较'
            
                
            
        else:
            descri_SAL_NPR='本期销售净利率与上期销售净利率无法比较'
            descri_SAL_GIR='本期销售毛利率与上期销售毛利率无法比较'
            descri_ROEA='本期净资产收益率与上期净资产收益率无法比较'
            descri_OR_YOY='本期营业收入同比增长率与上期营业收入同比增长率无法比较'
            descri_OP_YOY='本期营业利润同比增长率与上期营业利润同比增长率无法比较'
            descri_NP_YOY='本期净利润同比增长率与上期净利润同比增长率无法比较'
            descri_TA_RATE='本期总资产周转率与上期总资产周转率无法比较'
            descri_AP_RATE='本期应收账款周转率与上期应收账款周转率无法比较'
            descri_debt_to_ability='本期资产负债率与上期资产负债率无法比较'  
        
        '''
        现金流(经营活动现金净额、投资活动现金净额、筹资活动现金净额) 单位是亿元
        '''         
        #strsql="select a.end_date,a.CS_10000,a.CS_20000,a.CS_30000 from upcenter.FIN_CASH_SHORT a,upcenter.STK_BASIC_INFO b where a.COM_UNI_CODE=b.COM_UNI_CODE and b.stk_code="+"'"+stkcode+"'"+" and a.end_date>=to_date('20110331','yyyy-mm-dd') order by a.end_date desc" 
#        dfOra=dfOra[dfOra['END_DATE'].isin(date)]
        cash_flow=self._cash_flow[self._cash_flow['STK_CODE']==stkcode]#cash_flow用来画图
        if not isnan(list(cash_flow['CS_10000'])[0])and not isnan(list(cash_flow['CS_20000'])[0]) and not isnan(list(cash_flow['CS_30000'])[0]):
            if np.floor(list(cash_flow['CS_10000'])[0]+list(cash_flow['CS_20000'])[0]+list(cash_flow['CS_30000'])[0])>=1:
                descri_cash_flow='现金流状况良好'
            elif np.floor(list(cash_flow['CS_10000'])[0]+list(cash_flow['CS_20000'])[0]+list(cash_flow['CS_30000'])[0])==0:
                descri_cash_flow='现金流状况一般'
            elif np.floor(list(cash_flow['CS_10000'])[0]+list(cash_flow['CS_20000'])[0]+list(cash_flow['CS_30000'])[0])<=-1:
                descri_cash_flow='现金流紧张'
        else:
            descri_cash_flow='无法判断现金流状况'
        
        return  descri_cash_flow           
    '''
    @function 机构评级
    '''     
    def Insti_Rate(self,stkcode):
       #获取3个月内机构评级信息
        #strsql = "SELECT A.STK_CODE,A.ORG_UNI_CODE,B.ORG_CHI_SHORT_NAME,A.INDU_UNI_CODE,TO_CHAR(A.DECL_DATE,'YYYY-MM-DD') AS DECL_DATE,A.LAST_RATE_PAR,A.RES_RATE_PAR,A.RATE_CHG_PAR FROM UPCENTER.RES_STK_FORE_PRICE A JOIN UPCENTER.PUB_ORG_INFO B ON A.ORG_UNI_CODE = B.ORG_UNI_CODE WHERE DECL_DATE > TRUNC(SYSDATE)-90 AND A.ISVALID = 1 AND B.ISVALID = 1 AND A.STK_CODE ="+"'"+stkcode+"'"+"    ORDER BY A.DECL_DATE DESC"
#       strsql="SELECT C.STK_CODE,A.ORG_UNI_CODE,B.ORG_CHI_SHORT_NAME,A.INDU_UNI_CODE,TO_CHAR(A.DECL_DATE,'YYYY-MM-DD') AS DECL_DATE,A.LAST_RATE_PAR,A.RES_RATE_PAR,A.RATE_CHG_PAR FROM UPCENTER.RES_STK_FORE_PRICE A , UPCENTER.PUB_ORG_INFO B,UPCENTER. STK_BASIC_INFO C WHERE A.ORG_UNI_CODE = B.ORG_UNI_CODE AND A.STK_UNI_CODE=C.STK_UNI_CODE AND DECL_DATE > TRUNC(SYSDATE)-90 AND A.ISVALID = 1 AND B.ISVALID = 1 AND C.STK_CODE ="+"'"+stkcode+"'"+"  ORDER BY A.DECL_DATE DESC"
#       dfOra = pd.read_sql(strsql, self._dbcenter_conn_Ora)
#        strsql="SELECT A.RES_RATE_PAR FROM UPCENTER.RES_STK_FORE_PRICE A , UPCENTER.PUB_ORG_INFO B,UPCENTER. STK_BASIC_INFO C WHERE A.ORG_UNI_CODE = B.ORG_UNI_CODE AND A.STK_UNI_CODE=C.STK_UNI_CODE AND DECL_DATE > TRUNC(SYSDATE)-90 AND A.ISVALID = 1 AND B.ISVALID = 1 AND C.STK_CODE ="+"'"+stkcode+"'"+"  ORDER BY A.DECL_DATE DESC"
#        dfOra = pd.read_sql(strsql, self._dbcenter_conn_Ora)
        dfOra=self._insti_rate[self._insti_rate['STK_CODE']==stkcode]
        if not dfOra.empty:
            dfOra_buy=dfOra[dfOra['RES_RATE_PAR']==1]
            dfOra_incr=dfOra[dfOra['RES_RATE_PAR']==2]
            dfOra_mid=dfOra[dfOra['RES_RATE_PAR']==3]
            dfOra_decr=dfOra[dfOra['RES_RATE_PAR']==4]
            dfOra_sell=dfOra[dfOra['RES_RATE_PAR']==5]
            dfOra_other=dfOra[dfOra['RES_RATE_PAR']==6]
            if  dfOra_buy.shape[0]+dfOra_incr.shape[0]+ dfOra_mid.shape[0]+dfOra_decr.shape[0]+dfOra_sell.shape[0]+ dfOra_other.shape[0]!=0:
                mid_add=(dfOra_incr.shape[0]+ dfOra_buy.shape[0])/(dfOra_incr.shape[0]+dfOra_buy.shape[0]+dfOra_mid.shape[0]+dfOra_decr.shape[0]+dfOra_sell.shape[0])*100
                mid_mid=dfOra_mid.shape[0]/(dfOra_incr.shape[0]+dfOra_buy.shape[0]+dfOra_mid.shape[0]+dfOra_decr.shape[0]+dfOra_sell.shape[0])*100
                mid_sub=(dfOra_decr.shape[0]+dfOra_sell.shape[0])/(dfOra_incr.shape[0]+dfOra_buy.shape[0]+dfOra_mid.shape[0]+dfOra_decr.shape[0]+dfOra_sell.shape[0])*100
                if mid_add>=50:
                    descri='多数机构认为该股长期投资价值较高，投资者可加强关注.'
                elif mid_mid>=50:
                    descri='多数机构认为该股长期具有投资价值,投资者可持续观望.'
                elif mid_sub>=50:
                    descri='多数机构认为该股长期投资价值不高,投资者可给予较少的关注.'
            else:
                 descri='当前无可用数据'
        else:
             descri='当前无可用数据'
        return descri  
    
    '''
    @function 机构预测
    '''
    def Market_predict(self,stkcode):
        #strsql="select subj_avg from (select a.END_DATE,b.stk_code,FORE_YEAR,SUBJ_AVG,ROW_NUMBER() OVER (ORDER BY a.END_DATE desc) as rk  from upcenter.RES_COM_PROFIT_FORE a,upcenter.STK_BASIC_INFO b where a.isvalid=1 and a.SEC_UNI_CODE=b.STK_UNI_CODE and b.stk_code="+"'"+stkcode+"'"+"  and a.SUBJ_CODE=14 and FORE_YEAR=to_number(to_char(sysdate,'yyyy')) and STAT_RANGE_PAR=4 order by end_date desc,FORE_YEAR desc) where rk=1"
        #strsql="select a.END_DATE,b.stk_code,FORE_YEAR, SUBJ_AVG from upcenter.RES_COM_PROFIT_FORE a,upcenter.STK_BASIC_INFO b where a.isvalid=1 and a.SEC_UNI_CODE=b.STK_UNI_CODE and b.stk_code="+"'"+stkcode+"'"+"  and a.SUBJ_CODE=14 and FORE_YEAR>=to_number(to_char(sysdate,'yyyy'))-1 and STAT_RANGE_PAR=4 order by end_date desc,FORE_YEAR desc"
        dfOra=self._pre_eps[self._pre_eps['STK_CODE']==stkcode]
        #strsql="SELECT CLOSE_PRICE,STK_PER_TTM FROM (select b.TRADE_DATE,b.CLOSE_PRICE,b.STK_PER_TTM,row_number() over (ORDER BY b.TRADE_DATE DESC ) AS RK from upcenter.STK_BASIC_INFO a,upcenter.STK_BASIC_PRICE_MID b where a.STK_UNI_CODE=b.STK_UNI_CODE and a.stk_code="+"'"+stkcode+"'"+" and a.isvalid=1 and b.end_date=b.trade_date  order by b.Trade_Date desc) WHERE RK=1"
        dfOra_pe_close=self._pe_close[self._pe_close['STK_CODE']==stkcode]
        sysdate=datetime.date.today().year #系统时间
        if not dfOra.empty and not dfOra_pe_close.empty and list(dfOra['SUBJ_AVG'])[0]>0 and list(dfOra_pe_close['STK_PER_TTM'])[0]>0:
            pre_stkprice=round(list(dfOra['SUBJ_AVG'])[0]*list(dfOra_pe_close['STK_PER_TTM'])[0])
        else:
            pre_stkprice='--'
        if not isinstance(pre_stkprice,str):
            if  list(dfOra_pe_close['CLOSE_PRICE'])[0]>=pre_stkprice*(1+0.3):
                descri='机构预测'+str(sysdate)+'年的每股收益为'+str(list(dfOra['SUBJ_AVG'])[0])+',按当前市盈率计算,估值为'+str(pre_stkprice)+'元,当前股价被明显高估.'
            elif list(dfOra_pe_close['CLOSE_PRICE'])[0]<pre_stkprice*(1+0.3) and list(dfOra_pe_close['CLOSE_PRICE'])[0]>=pre_stkprice:
                descri='机构预测'+str(sysdate)+'年的每股收益为'+str(list(dfOra['SUBJ_AVG'])[0])+',按当前市盈率计算,估值为'+str(pre_stkprice)+'元,当前股价偏高.'
            elif list(dfOra_pe_close['CLOSE_PRICE'])[0]<pre_stkprice*(1-0.3):
                descri='机构预测'+str(sysdate)+'年的每股收益为'+str(list(dfOra['SUBJ_AVG'])[0])+',按当前市盈率计算,估值为'+str(pre_stkprice)+'元,当前股价被明显低估.'
            elif list(dfOra_pe_close['CLOSE_PRICE'])[0]>=pre_stkprice*(1-0.3) and (list(dfOra_pe_close['CLOSE_PRICE'])[0])<pre_stkprice:
                descri='机构预测'+str(sysdate)+'年的每股收益为'+str(list(dfOra['SUBJ_AVG'])[0])+',按当前市盈率计算,估值为'+str(pre_stkprice)+'元,当前股价偏低.'
        else:
            descri="当前无可用数据"
        return descri
        
         
    ''' 
    @function 给股票打分
    '''
    def Get_Stk_Star(self,stkcode):
        descri,rank_inc_i,rank_beps,rank_ps_ocf,rank_bps,num,descri_quali=self.Get_Value_Rank(stkcode)
        if sum([rank_inc_i!='--',rank_beps!='--' ,rank_ps_ocf!='--' ,rank_bps!='--'])==4:
            score=np.floor((rank_inc_i+rank_beps+rank_ps_ocf+rank_bps)*100/(4*num))
        elif sum([rank_inc_i!='--',rank_beps!='--' ,rank_ps_ocf!='--' ,rank_bps!='--'])==3:
            mid=[rank_inc_i,rank_beps,rank_ps_ocf,rank_bps]
            mid.remove('--')
            score=np.floor(sum(mid)*100/(num*3))
        elif sum([rank_inc_i!='--',rank_beps!='--' ,rank_ps_ocf!='--' ,rank_bps!='--'])==2:
            mid=[rank_inc_i,rank_beps,rank_ps_ocf,rank_bps]
            mid.remove('--')
            mid.remove('--')
            score=np.floor(sum(mid)*100/(num*2))
        elif sum([rank_inc_i!='--',rank_beps!='--' ,rank_ps_ocf!='--' ,rank_bps!='--'])==1:
            mid=[rank_inc_i,rank_beps,rank_ps_ocf,rank_bps]
            mid.remove('--')
            mid.remove('--')
            mid.remove('--')
            score=np.floor(sum(mid)*100/num)
        else:
            score=0
    
        if 0<score<=20:
            star=1
        elif score>20 and score<=40:
            star=2
        elif score>40 and score<=60:
            star=3
        elif score>60 and score<=80:
            star=4
        elif score>80 and score<=100:
            star=5
        else:
            star=0
        return star,score
        
    '''
    @function 得出结论
    '''
    def Get_Conclusion(self,stkcode):
        descri,rank_inc_i,rank_beps,rank_ps_ocf,rank_bps,num,descri_quali=self.Get_Value_Rank(stkcode)
        if descri_quali=='综合以上数据分析,公司质地优秀.':
            descri_1='公司质地优秀,在行业中处于领先水平.'
        elif descri_quali=='综合以上数据分析,公司质地良好.'or descri_quali=='综合以上数据分析,公司质地一般.' :
            descri_1='公司质地优秀,在行业中处于中等水平.'
        elif descri_quali=='综合以上数据分析,公司质地较差.':
            descri_1='公司质地优秀,在行业中处于落后水平.'
        else:
            descri_1=""
        descri_2=self.Insti_Rate(stkcode)
        descri=descri_1+descri_2
        return descri        
        
        
demo=Stock_Diagnosis_Data()
self=demo
self.Get_StkBaseInfo()

#stkcode='300625'
for stkcode in self._stkpool:
    print stkcode
    descri_conclusion=self.Get_Conclusion(stkcode) #得到结语
    print descri_conclusion
    #公司经营
    descri_manage,rank_inc_i,rank_beps,rank_ps_ocf,rank_bps,num,descri_quali=self.Get_Value_Rank(stkcode)
    print descri_manage
    descri_cash_flow=self.Company_Manage(stkcode)
    print descri_cash_flow
   
    #机构评级
    descri_insti=self.Insti_Rate(stkcode)
    print descri_insti
    #机构预测
    descri_insti_predict=self.Market_predict(stkcode)
    print descri_insti_predict
    #个股得分
    star,score=self.Get_Stk_Star(stkcode)
    print star
    #得到结论
    descri_coclusion=self.Get_Conclusion(stkcode)
    print descri_coclusion
toc()
