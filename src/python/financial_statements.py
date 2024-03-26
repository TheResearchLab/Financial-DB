
import requests
import pandas as pd
import sys
from db_connection import engine,api_key,session,text
from time import sleep
from tqdm import tqdm


def delete_oldest_entries(cache, max_size):
    while len(cache) > max_size:
        oldest_key = next(iter(cache)) 
        del cache[oldest_key]

cache = {}

def prep_financials_df(financials,code,exchange,ftype,freq):
    financials_df = pd.DataFrame.from_dict(financials['Financials'][ftype][freq]).T.reset_index(drop=True)
    financials_df['code'] = code
    financials_df['exchange'] = exchange
    financials_df['type'] = ftype
    financials_df['freq'] = freq
    return financials_df

def fetch_financials(code, exchange, ftype, freq):
    url = f"https://eodhd.com/api/fundamentals/{code}.{exchange}?api_token={api_key}&fmt=json"
    unique_combo = f'{code}-{exchange}'
    
    delete_oldest_entries(cache,6)
    empty_msg = f'stock ticker: {code} exchange: {exchange} ftype: {ftype} freq: {freq} is empty'
    
    if unique_combo in cache:
        financials = cache[unique_combo]
        financials_df = prep_financials_df(financials,code,exchange,ftype,freq)
        if financials_df.empty:
            print(empty_msg)
            return pd.DataFrame()
        return financials_df
    else:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                if int(response.headers['X-RateLimit-Remaining']) <= 1:
                    sleep(60)
                
                financials = response.json()
                financials_df = prep_financials_df(financials,code,exchange,ftype,freq)

                if financials_df.empty:
                    print(empty_msg)
                    return pd.DataFrame()
        

                cache[unique_combo] = financials
                return financials_df
        except Exception as e:
            raise Exception(f'Error has occurred: {e}')
            
        

def get_financials(target,bad_code):
    for code, exchange in tqdm(target,desc='processing',unit='ticker'):
        skip_code = False
        if f'{code}-{exchange}' in bad_code:
            continue
        for freq in ['quarterly', 'yearly']:
            for ftype in ['Balance_Sheet', 'Income_Statement', 'Cash_Flow']:
                
                financials_df = fetch_financials(code, exchange, ftype, freq)

                if financials_df.empty:
                    bad_code.append(f'{code}-{exchange}')
                    pd.DataFrame(bad_code,columns=['bad-tickers']).to_csv('bad_code.csv')
                    skip_code = True
                    break

                db_load_financials(financials_df)
            if skip_code:
                break 
        continue 
    return None

def db_load_financials(stmt):
    ftype = stmt['type'].iloc[0]
    freq = stmt['freq'].iloc[0]

    if ftype == 'Income_Statement' and freq == 'yearly':
        stmt.to_sql("stg_income_statement_yr",con=engine,if_exists='append',index=False)

    if ftype == 'Income_Statement' and freq == 'quarterly':
        stmt.to_sql("stg_income_statement_q",con=engine,if_exists='append',index=False)

    if ftype == 'Balance_Sheet' and freq == 'yearly':
        stmt.to_sql("stg_balance_sheet_yr",con=engine,if_exists='append',index=False)

    if ftype == 'Balance_Sheet' and freq == 'quarterly':
        stmt.to_sql("stg_balance_sheet_q",con=engine,if_exists='append',index=False)

    if ftype == 'Cash_Flow' and freq == 'yearly':
        stmt.to_sql("stg_cashflow_yr",con=engine,if_exists='append',index=False)

    if ftype == 'Cash_Flow' and freq == 'quarterly':
        stmt.to_sql("stg_cashflow_q",con=engine,if_exists='append',index=False)


if __name__ == '__main__':
    # trunc tables 
    # session.execute(text("TRUNCATE TABLE `the-research-lab-db`.`stg_income_statement_yr`"))
    # session.execute(text("TRUNCATE TABLE `the-research-lab-db`.`stg_income_statement_q`"))
    # session.execute(text("TRUNCATE TABLE `the-research-lab-db`.`stg_balance_sheet_q`"))
    # session.execute(text("TRUNCATE TABLE `the-research-lab-db`.`stg_balance_sheet_yr`"))
    # session.execute(text("TRUNCATE TABLE `the-research-lab-db`.`stg_cashflow_q`"))
    # session.execute(text("TRUNCATE TABLE `the-research-lab-db`.`stg_cashflow_yr`"))


    bad_codes = list(pd.read_csv('bad_code_keep.csv')['bad-tickers'])

    
    common_stock = pd.read_sql_query('select code, exchange_cd from stg_ticker_to_load',engine)
    ticker = common_stock['code'].reset_index(drop=True)
    exchange = common_stock['exchange_cd'].reset_index(drop=True)
    
    
    get_financials(list(zip(ticker,exchange)),bad_codes)




