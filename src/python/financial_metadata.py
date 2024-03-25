
import pandas as pd
from db_connection import api_key,engine,sessionmaker,text,session
import asyncio 
import aiohttp
import sys


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


# main function 
async def main():
    chunksize = 1000
    
    # trunc tables 
    session.execute(text("TRUNCATE TABLE `the-research-lab-db`.`stg_exchange`"))
    session.execute(text("TRUNCATE TABLE `the-research-lab-db`.`stg_ticker`"))

    # load financial asset codes and exchanges
    exchange_df = await get_exchange_df()
    ticker_df = await get_ticker_df(exchange_df.Code)
    
    # load exchange
    exchange_df.to_sql(name="stg_exchange",con=engine,if_exists="replace")

    # load financial asset codes
    for i in range(0, len(ticker_df), chunksize):
      chunk = ticker_df[i:i+chunksize]  # Get the chunk of data
      chunk.to_sql("stg_ticker", con=engine, if_exists='append', index=False)

   

    


    
    



if __name__ == "__main__":
    asyncio.run(main())
    session.close()

