import pandas as pd
import numpy as np
import statsmodels.formula.api as smf

class MonthEnd:
    def __init__(self, name, df_score, df_mktcap, df_ret):
        self.name = name
        self.df_score = df_score
        self.df_ret = df_ret
        self.df_mktcap = df_mktcap
        df_rolling_score = pd.DataFrame()
        df_rolling_score = pd.DataFrame(index=self.df_score.index)
        self.df_rolling_score = df_rolling_score
        self.df_quantiled = pd.DataFrame()
        self.df_q1 = pd.DataFrame()
        self.df_q2 = pd.DataFrame()
        self.df_q3 = pd.DataFrame()
        self.df_q4 = pd.DataFrame()
        self.df_q5 = pd.DataFrame()
        self.df_q1_merged = pd.DataFrame()
        self.df_q2_merged = pd.DataFrame()
        self.df_q3_merged = pd.DataFrame()
        self.df_q4_merged = pd.DataFrame()
        self.df_q5_merged = pd.DataFrame()
        self.df_port_ret =pd.DataFrame()
        self.q1_port_ret = 0
        self.q2_port_ret = 0
        self.q3_port_ret = 0
        self.q4_port_ret = 0
        self.q5_port_ret = 0

# numpy is key for doing vector operations and not using slow "for" loop. Note np.fmax ignores nan
    def rolling_scores(self,month_1,month_2):
        temp= np.fmax(month_1.df_score['scores'],month_2.df_score['scores'])
        self.df_rolling_score['scores']=np.fmax(self.df_score['scores'],temp)

    def quintles(self,df):
        self.df_quantiled['scores'] = pd.qcut(df['scores'],5, labels = False)
        self.df_q1 = self.df_quantiled[self.df_quantiled['scores'] == 0]
        self.df_q2 = self.df_quantiled[self.df_quantiled['scores'] == 1]
        self.df_q3 = self.df_quantiled[self.df_quantiled['scores'] == 2]
        self.df_q4 = self.df_quantiled[self.df_quantiled['scores'] == 3]
        self.df_q5 = self.df_quantiled[self.df_quantiled['scores'] == 4]


    def Quintile_MktCap_Ret_Merged(self,prev_month):
        temp = pd.DataFrame()
        temp = prev_month.df_q1.merge(prev_month.df_mktcap , on='CIK')
        self.df_q1_merged = temp.merge(self.df_ret, on ='CIK')
        total = self.df_q1_merged['Mktcap'].sum()
        self.df_q1_merged['Mktcap_wt'] =self.df_q1_merged['Mktcap']/total
        self.df_q1_merged['Mktcap_wt_ret']=np.multiply(self.df_q1_merged['Mktcap_wt'],self.df_q1_merged['RET'])
        self.q1_port_ret = self.df_q1_merged['Mktcap_wt_ret'].sum()

        temp = self.df_q2.merge(prev_month.df_mktcap, on='CIK')
        self.df_q2_merged = temp.merge(self.df_ret, on='CIK')
        total = self.df_q2_merged['Mktcap'].sum()
        self.df_q2_merged['Mktcap_wt'] = self.df_q2_merged['Mktcap'] / total
        self.df_q2_merged['Mktcap_wt_ret'] = np.multiply(self.df_q2_merged['Mktcap_wt'], self.df_q2_merged['RET'])
        self.q2_port_ret = self.df_q2_merged['Mktcap_wt_ret'].sum()

        temp = self.df_q3.merge(prev_month.df_mktcap, on='CIK')
        self.df_q3_merged = temp.merge(self.df_ret, on='CIK')
        total = self.df_q3_merged['Mktcap'].sum()
        self.df_q3_merged['Mktcap_wt'] = self.df_q3_merged['Mktcap'] / total
        self.df_q3_merged['Mktcap_wt_ret'] = np.multiply(self.df_q3_merged['Mktcap_wt'], self.df_q3_merged['RET'])
        self.q3_port_ret = self.df_q3_merged['Mktcap_wt_ret'].sum()

        temp = self.df_q4.merge(prev_month.df_mktcap, on='CIK')
        self.df_q4_merged = temp.merge(self.df_ret, on='CIK')
        total = self.df_q4_merged['Mktcap'].sum()
        self.df_q4_merged['Mktcap_wt'] = self.df_q4_merged['Mktcap'] / total
        self.df_q4_merged['Mktcap_wt_ret'] = np.multiply(self.df_q4_merged['Mktcap_wt'], self.df_q4_merged['RET'])
        self.q4_port_ret = self.df_q4_merged['Mktcap_wt_ret'].sum()

        temp = self.df_q5.merge(prev_month.df_mktcap, on='CIK')
        self.df_q5_merged = temp.merge(self.df_ret, on='CIK')
        total = self.df_q5_merged['Mktcap'].sum()
        self.df_q5_merged['Mktcap_wt'] = self.df_q5_merged['Mktcap'] / total
        self.df_q5_merged['Mktcap_wt_ret'] = np.multiply(self.df_q5_merged['Mktcap_wt'], self.df_q5_merged['RET'])
        self.q5_port_ret = self.df_q5_merged['Mktcap_wt_ret'].sum()

# load month ends into list
with open('C:/Users/Kamesh/Desktop/Geczy/OOPS/Month_End.csv') as f:
    Month_End = f.read().splitlines()
    Month_End = [int(i) for i in Month_End]
f.close()

# 1.load scores into dataframes
df= pd.read_csv('C:/Users/Kamesh/Desktop/Geczy/OOPS/all_filtered_for_available_CUSIPS_Cosine.csv')
df_scores = pd.pivot_table(df, values= 'Cosine', index= 'CIK', columns= 'Month_End',aggfunc= 'mean')

#df_scores.to_csv('C:/Users/Kamesh/Desktop/Geczy/OOPS/Output/scores.csv')

# 1. compute marketcap
# 2. clean returns
# 3. load marketcap and returns into dataframe
df= pd.read_csv('C:/Users/Kamesh/Desktop/Geczy/OOPS/f3f3c3d75873dd01_1994_2018_CIK.csv', usecols = ['CIK', 'CUSIP','date','PRC','RET','SHROUT'])
#df['new_date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d')
df['Mktcap']= abs(df.PRC * df.SHROUT)
df.set_index('CIK', inplace=True)
df['RET'] = df['RET'].replace('C', np.nan)
df['RET'] = df['RET'].replace('B', np.nan)
df.RET = df.RET.astype(float)

df_mktcap = pd.pivot_table(df, values= 'Mktcap', index= 'CIK', columns= 'date',aggfunc= 'mean')
df_ret = pd.pivot_table(df, values= 'RET', index= 'CIK', columns= 'date',aggfunc= 'mean')

df_mktcap.to_csv('C:/Users/Kamesh/Desktop/Geczy/OOPS/Output/mktcap.csv')
df_ret.to_csv('C:/Users/Kamesh/Desktop/Geczy/OOPS/Output/ret.csv')

# create a list to store created objects
ObjectList=[]

for month in Month_End:
    for column_scores in df_scores.columns:
        if (column_scores == month):
            for column_mktcap in df_mktcap.columns:
                if (column_mktcap == month):
                    for column_ret in df_ret.columns:
                        if (column_ret == month):
                            df_score_monthend = pd.DataFrame()
                            df_score_monthend = pd.DataFrame(index=df_scores.index)
                            df_score_monthend['scores']=df_scores[column_scores]

                            df_mktcap_monthend = pd.DataFrame()
                            df_mktcap_monthend = pd.DataFrame(index=df_mktcap.index)
                            df_mktcap_monthend['Mktcap'] = df_mktcap[column_mktcap]

                            df_ret_monthend = pd.DataFrame()
                            df_ret_monthend = pd.DataFrame(index=df_ret.index)
                            df_ret_monthend['RET'] = df_ret[column_ret]

                            ObjectList.append(MonthEnd(month,df_score_monthend,df_mktcap_monthend,df_ret_monthend))
                        else:
                            continue
                else:
                    continue
        else:
            continue

for i in range(2, len(ObjectList)):
    ObjectList[i].rolling_scores(ObjectList[i-2],ObjectList[i-1])

for i in range(2, len(ObjectList)):
    ObjectList[i].quintles(ObjectList[i].df_rolling_score)

for i in range(3, len(ObjectList)):
    ObjectList[i].Quintile_MktCap_Ret_Merged(ObjectList[i-1])

def Regression(Quintile):

    def switch (Quintile):
        switcher = {
            'Q1' : "q1_port_ret",
            'Q2' : "q2_port_ret",
            'Q3' : "q3_port_ret",
            'Q4' : "q4_port_ret",
            'Q5' : "q5_port_ret"
        }
        return switcher.get(Quintile, "Invalid Quintile")

    df_quintile = pd.DataFrame()

    df_famafrench = pd.read_csv('C:/Users/Kamesh/Desktop/Geczy/OOPS/F-F_Research_Data_Factors.csv')
    a = switch(Quintile)
    for i in range(3, len(ObjectList)):
        data = pd.DataFrame({"month": [ObjectList[i].name], "port_ret": [ObjectList[i].q5_port_ret]})
        df_quintile = df_quintile.append(data)

    df_quintile_merged = df_quintile.merge(df_famafrench, on='month')
    df_quintile_merged['excess_ret'] = df_quintile_merged['port_ret'] - df_quintile_merged['RF']

    df_Regression = pd.DataFrame(df_quintile_merged, columns=['Mkt_RF', 'SMB', 'HML', 'excess_ret'])
    #df_Regression.to_csv('C:/Users/Kamesh/Desktop/Geczy/OOPS/Output/Regression.csv')

    model = smf.ols(formula='excess_ret~ Mkt_RF + SMB + HML', data=df_Regression)

    # white heterskedasticity robust standard errors
    results_formula = model.fit(cov_type='HC3')

    # print(dir(results_formula))
    print(results_formula.tvalues)

Regression('Q1')

# # Run regression - need to write a function and call each quintile
# df_quintile1=pd.DataFrame()
#
# df_famafrench= pd.read_csv('C:/Users/Kamesh/Desktop/Geczy/OOPS/F-F_Research_Data_Factors.csv')
#
# for i in range(3, len(ObjectList)):
#     data = pd.DataFrame({"month": [ObjectList[i].name], "port_ret":[ObjectList[i].q1_port_ret]})
#     df_quintile1 = df_quintile1.append(data)
#
# df_quintile1_merged = df_quintile1.merge(df_famafrench , on='month')
# df_quintile1_merged['excess_ret'] =df_quintile1_merged['port_ret']-df_quintile1_merged['RF']
#
# df_Regression = pd.DataFrame(df_quintile1_merged,columns=['Mkt_RF','SMB','HML', 'excess_ret'])
# df_Regression.to_csv('C:/Users/Kamesh/Desktop/Geczy/OOPS/Output/Regression.csv')
#
# #df_Y = pd.DataFrame(df_quintile1_merged,columns='excess_ret')
# model = smf.ols(formula= 'excess_ret~ Mkt_RF + SMB + HML', data=df_Regression)
# # white heterskedasticity robust standard errors
# results_formula = model.fit(cov_type='HC3')
# #print(dir(results_formula))
# print(results_formula.tvalues)

