import ccxt
import pandas as pd
import numpy as np
import datetime
import time
import requests

# ===== 全局设置 =====
API_KEY = '2qQyTVjIsPnKqL8I33jJnLLs67I'
MAX_POS = 0.005  # 最大持仓量 (BTC)
EXCHANGE = ccxt.bybit({
    'apiKey': 'AFdnF6qim8psBUCfAL',
    'secret': 'dkgiiBX1JoHD86EmP1WpNkHUhOGvhhPTCSIV',
    'proxies': {
        'http': 'http://127.0.0.1:15236',
        'https': 'http://127.0.0.1:15236'
    }
})
SYMBOL = 'BTCUSDT'

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


# ===== 数据获取函数 =====
def fetch_data(metric_url, asset):
    """
    获取指标数据和BTC价格数据，并合并。
    """
    since = 1733072381 #1672502400  # 数据起始时间：2023年1月1日
    until = int(time.time())
    resolution = "1h"

    # 获取指标数据
    res_value = requests.get(metric_url, params={
        "a": asset,
        "s": since,
        "u": until,
        "api_key": API_KEY,
        "i": resolution
    })
    df_value = pd.read_json(res_value.text, convert_dates=['t'])

    # 获取BTC价格数据
    res_price = requests.get("https://api.glassnode.com/v1/metrics/market/price_usd_close", params={
        "a": "BTC",
        "s": since,
        "u": until,
        "api_key": API_KEY,
        "i": resolution
    })
    df_price = pd.read_json(res_price.text, convert_dates=['t'])

    # 合并数据
    df = pd.merge(df_value, df_price, how='inner', on='t')
    df = df.rename(columns={'v_x': 'value', 'v_y': 'price'})
    return df


# ===== 数据更新函数 =====
def update_data_file(metric_url, asset, file_name):
    """
    更新指定指标数据文件。
    """
    try:
        df_new = fetch_data(metric_url, asset)
        df_new = pd.DataFrame(df_new, columns=['t', 'value', 'price'])

        try:
            df_old = pd.read_csv(file_name)
            df_new = pd.concat([df_old, df_new]).drop_duplicates(subset=['t']).reset_index(drop=True)
        except FileNotFoundError:
            pass  # 如果文件不存在，直接使用新数据

        df_new.to_csv(file_name, index=False)
    except Exception as e:
        print(f"Error updating file {file_name}: {e}")


# ===== 策略函数 =====
def strat_1(x, y):
    """
    策略1：使用ETH价格数据预测BTC价格。
    """
    df = pd.read_csv('gn_data_1.csv')
    df['pct_change'] = df['price'].pct_change()
    df['ma'] = df['value'].rolling(x).mean()
    df['sd'] = df['value'].rolling(x).std()
    df['z'] = (df['value'] - df['ma']) / df['sd']
    df['pos'] = np.where(df['z'] > y, 1, 0)
    return df['pos'].iloc[-1]


def strat_2(x, y):
    """
    策略2：基于USDC余额分布预测BTC价格。
    """
    df = pd.read_csv('gn_data_2.csv')
    df['pct_change'] = df['price'].pct_change()
    df['min'] = df['value'].rolling(x).min()
    df['max'] = df['value'].rolling(x).max()
    df['pos'] = np.where((df['value'] - df['min']) / (df['max'] - df['min']) > y, 1, -1)
    return df['pos'].iloc[-1]


def calculate_position():
    """
    计算总体仓位。
    """
    pos_1 = strat_1(800, 0.5)
    pos_2 = strat_2(3000, 0.3)
    pos = pos_1 * 0.5 + pos_2 * 0.5

    # 保存信号
    try:
        df_signal = pd.read_csv('signal.csv')
    except FileNotFoundError:
        df_signal = pd.DataFrame(columns=['dt', 'pos'])

    new_row = pd.DataFrame([[datetime.datetime.now(), pos]], columns=['dt', 'pos'])
    df_signal = pd.concat([df_signal, new_row], ignore_index=True)
    df_signal.to_csv('signal.csv', index=False)
    print(pos)
    return pos


# ===== 交易函数 =====
def current_pos():
    """
    获取当前持仓。
    """
    position = EXCHANGE.fetch_position(SYMBOL)['info']
    if position['side'] == 'Buy':
        return float(position['size'])
    elif position['side'] == 'Sell':
        return -float(position['size'])
    return 0


def execute_trade(signal):
    """
    根据信号执行交易。
    """
    net_pos = current_pos()
    target_pos = MAX_POS * signal
    bet_size = round(target_pos - net_pos, 3)

    try:
        if bet_size > 0:
            EXCHANGE.create_order(SYMBOL, 'market', 'buy', bet_size, None)
        elif bet_size < 0:
            EXCHANGE.create_order(SYMBOL, 'market', 'sell', abs(bet_size), None)
    except Exception as e:
        print(f"Error executing trade: {e}")


# ===== 主循环 =====
import threading

# ===== 主循环 =====
def main():
    while True:
        now = datetime.datetime.now()

        # 每小时3分同时更新数据
        if now.minute == 26 and now.second == 0:
            # 定义线程
            thread_1 = threading.Thread(
                target=update_data_file,
                args=("https://api.glassnode.com/v1/metrics/market/price_usd_close", "ETH", "gn_data_1.csv")
            )
            thread_2 = threading.Thread(
                target=update_data_file,
                args=("https://api.glassnode.com/v1/metrics/distribution/balance_exchanges", "USDC", "gn_data_2.csv")
            )

            # 启动线程
            thread_1.start()
            thread_2.start()

            # 等待线程完成
            thread_1.join()
            thread_2.join()

            # 更新仓位
            calculate_position()

            df = pd.read_csv('signal.csv')
            signal = df['pos'].iloc[-1]
            execute_trade(signal)

        time.sleep(1)



if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("程序终止")