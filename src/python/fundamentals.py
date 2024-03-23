
import pandas as pd
from db_connection import engine,session,api_key,text
import asyncio 
import aiohttp


async def get_exchange_df() -> pd.DataFrame:
    url = f'https://eodhd.com/api/exchanges-list/?api_token={api_key}&fmt=json'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            exchange_list = await response.json()
    return pd.DataFrame.from_dict(exchange_list)

async def fetch_ticker(session, exchange):
    url = f'https://eodhd.com/api/exchange-symbol-list/{exchange}?api_token={api_key}&fmt=json'
    async with session.get(url) as response:
        ticker_list = await response.json()
        df = pd.DataFrame.from_dict(ticker_list)
        df['exchange_cd'] = exchange
    return df

async def get_ticker_df(exchange_list: pd.Series) -> pd.DataFrame:
    ticker_df = pd.DataFrame()
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_ticker(session,exchange) for exchange in exchange_list]
        ticker_dfs = await asyncio.gather(*tasks)
        ticker_df = pd.concat(ticker_dfs)
    return ticker_df.reset_index(drop=True)


async def fetch_financials(session,code,exchange,type,freq):
    url = f"https://eodhd.com/api/fundamentals/{code}.{exchange}?api_token={api_key}&fmt=json"
    async with session.get(url) as response:
        financials = await response.json()
        financials_df = pd.DataFrame.from_dict(financials['Financials'][type][freq]).T.reset_index(drop=True)
        financials_df['code'] = code
        financials_df['exchange'] = exchange
    return financials_df
        

async def get_financial_df(code_list:pd.Series,exchange_list:pd.Series,type,freq) -> pd.DataFrame:
    financial_df = pd.DataFrame()
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_financials(session,code,exchange,type,freq) for code, exchange in zip(code_list,exchange_list)]
        financial_dfs = await asyncio.gather(*tasks)
        financial_df = pd.concat(financial_dfs)
    return financial_df.reset_index(drop=True)




# main function 
async def main():
    chunksize = 1000
    
    # trunc tables 
    #session.execute(text("TRUNCATE TABLE `the-research-lab-db`.`stg_exchange`"))
    #session.execute(text("TRUNCATE TABLE `the-research-lab-db`.`stg_ticker`"))

    # load financial asset codes and exchanges
    exchange_df = await get_exchange_df()
    ticker_df = await get_ticker_df(exchange_df.Code)

    # load common stock balance sheet, income statement, and cashflow statements
    cs_ticker_cd = ticker_df[ticker_df['Type'] == 'Common Stock'].Code
    cs_exchange_cd = ticker_df[ticker_df['Type'] == 'Common Stock'].exchange_cd
    cs_balance_sheet_df = await get_financial_df(cs_ticker_cd,cs_exchange_cd,'Balance_Sheet','quarterly')


    # load exchange
    # exchange_df.to_sql(name="stg_exchange",con=engine,if_exists="replace")

    # load financial asset codes
    # for i in range(0, len(ticker_df), chunksize):
    #   chunk = ticker_df[i:i+chunksize]  # Get the chunk of data
    #   chunk.to_sql("stg_ticker", con=engine, if_exists='append', index=False)

    # load common stock balance sheet
    for i in range(0,len(cs_balance_sheet_df),chunksize):
        chunk = cs_balance_sheet_df[i:i+chunksize]
        chunk.to_sql("stg_balance_sheet_q",con=engine,if_exists='append',index=False)


    
    



if __name__ == "__main__":
    asyncio.run(main())
    session.close()

#%%
from db_connection import api_key
import requests
url = f"https://eodhd.com/api/fundamentals/A.US?api_token={api_key}&fmt=json"

response = requests.get(url)
data = response.json()
#data['Financials']['Balance_Sheet']['quarterly']
#data['Financials']['Balance_Sheet']['yearly']

#data['Financials']['Cash_Flow']['quarterly']
#data['Financials']['Cash_Flow']['yearly']

#data['Financials']['Income_Statement']['quarterly']
pd.DataFrame.from_dict(data['Financials']['Income_Statement']['yearly']).T.reset_index(drop=True).columns



# %%