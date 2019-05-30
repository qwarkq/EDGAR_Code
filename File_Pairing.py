import pandas as pd
from tqdm import tqdm

# input file with 10K/10Q text file names and filing date from https://sraf.nd.edu/data/ and convert date to correct format
df = pd.read_csv("C:/Users/Kamesh/Desktop/Kamesh/Test/LM_10X_Summaries_2018_Kamesh_Calendar.csv")
df['FILING_DATE'] = pd.to_datetime(df['FILING_DATE'])


# Open previously created csv file with a list of month end dates from 1994-2018 and load into list
with open('C:/Users/Kamesh/Desktop/Kamesh/Month_End.csv') as f:
    Month_End = f.read().splitlines()
f.close()

# open output file to which the file pairs will be written and write header names to file as first row
fout = open('C:/Users/Kamesh/Desktop/Kamesh/Test/File_Pairs.csv', "a+")

headers = "".join(['CIK', ',','Month_End', ',','QuarterA', ',','YearA', ',','File_nameA', ',', 'QuarterB', ',', 'YearB', ',', 'File_nameB'])
fout.write(headers)
fout.write('\n')

# For each month end in list identify all companies that have filed in that month and pair with the filing in same quarter previous year
for month_end in tqdm(range(12, len(Month_End)), desc='months'):
        # df2 identifies also companies filed in that month end
        df2 = df[(df.FILING_DATE <= Month_End[month_end])& (df.FILING_DATE > Month_End[month_end-1])]
        for index, row in df2.iterrows():
            fye = row['FYE']
            frm = row['FORM_TYPE']
            qtr = row['Quarter']
            yr = row['Year']
            cik = row['CIK']
            fye_month = row['FYE_MONTH']

            if frm == '10-Q':
                yr_previous = yr - 1
            elif frm == '10-K':
                yr_previous = yr - 1
            else:
                continue
            # df3 filters for all filings in same quarter but previous year for companies identifed in df2
            df3 = df[(df.CIK == cik) & (df.Year == yr_previous) & (df.FORM_TYPE == frm) & (df.Quarter == qtr)]
            df3.reset_index(inplace=True)

            if df3.empty:
                continue
            else:
                # Note, the first file [index (0)] is picked if multiple filings are present in same quarter of previous year for a company
                my_string = "".join([str(row['CIK']), ',', str(Month_End[month_end]), ',', str(row['Quarter']), ',', str(row['Year']), ',', str(row['File_name']), ',', str(df3.loc[0, 'Quarter']), ',', str(df3.loc[0, 'Year']), ',', str(df3.loc[0, 'File_name'])])
                fout.write(my_string)
                fout.write('\n')

fout.close()