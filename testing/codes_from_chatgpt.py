import pandas as pd
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne
import time
import akshare as ak

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017/')
db = client['stock_data']
collection = db['daily_kline']
status_collection = db['data_update_status']

def add_prefix(stock_code):
    return f'sh{stock_code}' if stock_code.startswith('6') else f'sz{stock_code}'

def fetch_data_with_retry(stock_code, start_date=None, end_date=None, retries=3, delay=5):
    for attempt in range(retries):
        try:
            if start_date is None:
                stock_data = ak.stock_zh_a_hist(symbol=stock_code, period="daily", adjust="qfq")
            else:
                start_date = pd.to_datetime(start_date).strftime('%Y%m%d')
                end_date = pd.to_datetime(end_date).strftime('%Y%m%d') if end_date else datetime.today().strftime('%Y%m%d')
                stock_data = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            stock_data.rename(columns={
                '日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low', 
                '收盘': 'close', '成交量': 'volume', '成交额': 'amount', '振幅': 'amplitude',
                '换手率': 'turnover', '涨跌幅': 'pct_chg'}, inplace=True)
            stock_data['date'] = pd.to_datetime(stock_data['date'])
            return stock_data
        except Exception as e:
            print(f"获取 {stock_code} 数据失败: {e}")
            if attempt == retries - 1:
                raise
            else:
                print("重试...")
                time.sleep(delay)

def convert_to_datetime(date):
    if isinstance(date, datetime):
        return date
    elif isinstance(date, str):
        return pd.to_datetime(date)
    elif isinstance(date, pd.Timestamp):
        return date.to_pydatetime()
    else:
        raise TypeError("Unsupported date type.")

def read_update_status():
    update_status_df = pd.DataFrame(list(status_collection.find({}, {'_id': 0, 'code': 1, 'name': 1, 'start_date': 1, 'end_date': 1})))
    return update_status_df

def insert_data(stock_code, stock_data):
    try:
        stock_info = ak.stock_individual_info_em(symbol=stock_code)
        stock_name = stock_info['股票简称'][0]
        stock_data['code'] = stock_code[2:]  # 去掉前缀的股票代码
        stock_data['name'] = stock_name
        
        updates = []
        for record in stock_data.to_dict('records'):
            record['date'] = convert_to_datetime(record['date'])
            updates.append(UpdateOne(
                {'code': record['code'], 'date': record['date']},
                {'$set': record},
                upsert=True
            ))
        collection.bulk_write(updates)
        
        # 更新data_update_status表
        new_end_date = stock_data['date'].max().strftime('%Y-%m-%d')
        status_collection.update_one(
            {'code': stock_code[2:]},
            {'$set': {'end_date': new_end_date, 'name': stock_name}},
            upsert=True
        )
    except Exception as e:
        print(f"插入 {stock_code} 数据失败: {e}")

def complete_data():
    update_status_df = read_update_status()
    today = datetime.today().strftime('%Y-%m-%d')
    
    for _, row in update_status_df.iterrows():
        code = row['code']
        end_date = row['end_date']
        start_date = (pd.to_datetime(end_date) + timedelta(days=1)).strftime('%Y-%m-%d')
        
        prefixed_code = add_prefix(code)
        
        try:
            stock_data = fetch_data_with_retry(prefixed_code, start_date=start_date, end_date=today)
            
            if stock_data.empty:
                print(f"No new data for {code}")
                continue
            
            insert_data(prefixed_code, stock_data)
        except Exception as e:
            print(f"补全 {code} 数据失败: {e}")

def resample_to_weekly(data):
    data = data.set_index('date')
    weekly_data = data.resample('W').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'code': 'first',
        'name': 'first'
    }).dropna()
    return weekly_data.reset_index()[['code', 'name', 'date', 'open', 'high', 'low', 'close', 'volume']]

def calculate_moving_averages(data):
    data = data.set_index('date')
    data['5_week_ma'] = data['close'].rolling(window=5).mean()
    data['20_week_ma'] = data['close'].rolling(window=20).mean()
    return data.reset_index()

def fetch_data(stock_code, start_date=None, end_date=None):
    prefixed_code = add_prefix(stock_code)
    return fetch_data_with_retry(prefixed_code, start_date=start_date, end_date=end_date)

def query_data(stock_code, start_date=None, end_date=None):
    query = {'code': stock_code}
    if start_date and end_date:
        query['date'] = {'$gte': pd.to_datetime(start_date), '$lte': pd.to_datetime(end_date)}
    elif start_date:
        query['date'] = {'$gte': pd.to_datetime(start_date)}
    elif end_date:
        query['date'] = {'$lte': pd.to_datetime(end_date)}
    
    cursor = collection.find(query, {'_id': 0}).sort('date', 1)
    return pd.DataFrame(list(cursor))

# 示例：补全数据
complete_data()

# 示例：获取某个股票的数据
data = query_data('000001', start_date='2024-06-20', end_date='2024-06-21')
print(data)

# 示例：计算周均线
weekly_data = resample_to_weekly(data)
ma_data = calculate_moving_averages(weekly_data)
print(ma_data)
