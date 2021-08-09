import pandas as pd
import requests
from bs4 import BeautifulSoup

def load_df_from_excel(excel_file, sheet):
    df = pd.read_excel(excel_file,sheet_name=sheet)
    return df['Research-Ratings-Link'].tolist()

#c = load_df_from_excel('SPDR Select-Holdings.xlsx','XLK (Technology)')

def scrape(excel_file, sheet):
    headers = {
        'User-Agent':
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML,like Gecko) Chrome/41.0.2228.0 Safari/537.36'}
    TargetHigh = []
    TargetMedian = []	
    TargetLow = []	
    TargetAverage = []	
    Consensus3M = []	
    Consensus1M = []
    ConsensusCurrent = []	
    Overweight3M = []	
    Overweight1M = []	
    OverweightCurrent = []	
    Hold3M	 = []
    Hold1M = []	
    HoldCurrent = []	
    Underweight3M = []
    Underweight1M = []
    UnderweightCurrent = []	
    Sell3M	= []
    Sell1M = []
    SellCurrent = []
    Research_Ratings = []
    

    urls = load_df_from_excel(excel_file, sheet)
    for url in urls:
        print(f'scrapping for {url}')
        read_site = requests.get(url,headers=headers)
        content = BeautifulSoup(read_site.text,'html.parser')
        target = content.find('div',class_ ='cr_data rr_stockprice module')
        try:
            if target != []:
                TargetHigh.append(target.find_all('span',class_ = 'data_data')[0].text)
                TargetMedian.append(target.find_all('span',class_ = 'data_data')[1].text)
                TargetLow.append(target.find_all('span',class_ = 'data_data')[2].text)
                TargetAverage.append(target.find_all('span',class_ = 'data_data')[3].text)
            else:
                pass
            ratings = content.find('div',class_='cr_analystRatings cr_data module')
            if ratings != []:
                Overweight3M.append(ratings.find_all('span',class_ = 'data_data')[3].text)
                Overweight1M.append(ratings.find_all('span',class_ = 'data_data')[4].text)
                OverweightCurrent.append(ratings.find_all('span',class_ = 'data_data')[5].text)
                Hold3M.append(ratings.find_all('span',class_ = 'data_data')[6].text)
                Hold1M.append(ratings.find_all('span',class_ = 'data_data')[7].text)
                HoldCurrent.append(ratings.find_all('span',class_ = 'data_data')[8].text)
                Underweight3M.append(ratings.find_all('span',class_ = 'data_data')[9].text)
                Underweight1M.append(ratings.find_all('span',class_ = 'data_data')[10].text)
                UnderweightCurrent.append(ratings.find_all('span',class_ = 'data_data')[11].text)
                Sell3M.append(ratings.find_all('span',class_ = 'data_data')[12].text)
                Sell1M.append(ratings.find_all('span',class_ = 'data_data')[13].text)
                SellCurrent.append(ratings.find_all('span',class_ = 'data_data')[14].text)
                Consensus3M.append(ratings.find_all('div','numValue-content')[0].text)
                Consensus1M.append(ratings.find_all('div','numValue-content')[1].text)
                ConsensusCurrent.append(ratings.find_all('div','numValue-content')[2].text)
                Research_Ratings.append(url)
            else:
                break
        except:
            pass

        data_info = {
            'Research_Ratings': Research_Ratings,
            'TargetHigh' : TargetHigh,
            'TargetMedian' : TargetMedian,
            'TargetLow' : TargetLow,
            'TargetAverage' : TargetAverage,	
            'Consensus3M' : Consensus3M,	
            'Consensus1M' : Consensus1M,
            'ConsensusCurrent' : ConsensusCurrent,
            'Overweight3M' : Overweight3M,	
            'Overweight1M' : Overweight1M,	
            'OverweightCurrent' : OverweightCurrent,	
            'Hold3M'	 : Hold3M,
            'Hold1M' : Hold1M,	
            'HoldCurrent' : HoldCurrent,	
            'Underweight3M' : Underweight3M,
            'Underweight1M' : Underweight1M,
            'UnderweightCurrent' : UnderweightCurrent,	
            'Sell3M'	: Sell3M,
            'Sell1M' : Sell1M,
            'SellCurrent' : SellCurrent

        }
        df = pd.DataFrame(data_info)
    return df


data = scrape('SPDR Select-Holdings.xlsx','NASDAQ 100 INDEX COMPANIES')
data.to_csv('NASAQ.csv',index=False)