"""
获取相关数据的模块：
- gate/binance 实时资金费率，以及下次的资金费率
- 合并g/b的实时资金费率，并计算差额
- gate/binance 各symbol的细节，例如min_qty, quanto_multiplier
- 根据amount，计算g/b上各自下单的设置，
    - b包括：symbol, qty
    - g包括：symbol, size
"""

import pandas as pd
import numpy as np
import math
from binance.client import Client
from gate_api import FuturesApi, Configuration, ApiClient
from config import *

pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.max_rows', None)     # 显示所有行
pd.set_option('display.width', 1000)        # 设置显示宽度
pd.set_option('display.max_colwidth', None) # 设置列内容的最大宽度

class BinanceDataHandler:

    def __init__(self, api_key=None, api_secret=None):

        self.client = Client(api_key, api_secret,
                             requests_params={
                'proxies': {
                    'http': BINANCE_PROXY,
                    'https': BINANCE_PROXY,
                    }
                })

    # Binance所有合约的实时funding rate，以及下次funding生效时间
    def bi_get_funding_rates(self):

        # Get funding rate
        df = pd.DataFrame(self.client.futures_mark_price())
        df = df[['symbol','markPrice','lastFundingRate','nextFundingTime','time']]

        df['lastFundingRate'] = df['lastFundingRate'].astype(float)
        df['markPrice'] = df['markPrice'].astype(float)
        df['nextFundingTime'] = pd.to_datetime(df['nextFundingTime'], unit='ms')
        df['time'] = pd.to_datetime(df['time'], unit='ms')

        # Get status and filter for only TRADING
        contract_status = self.bi_get_all_contract_status()  # 包含 status 信息
        df = df.merge(contract_status[['symbol', 'status']], on='symbol', how='left')
        df = df[df['status'] == 'TRADING']

        # Sort by funding rate (descending)
        df.sort_values(by="lastFundingRate", ascending=False, inplace=True)

        return df

    # Binance单个合约的实时funding rate
    def get_funding_rate(self, symbol):
        try:
            symbol_info = self.client.futures_mark_price(symbol=symbol)
            return symbol_info['lastFundingRate']
        except Exception as e:
            print(f"[Binance FR] 获取 {symbol} 资金费率失败: {e}")
            return 0.0001

    # Binance上获取某个合约的实时价格
    def bi_get_price(self, symbol='BTCUSDT'):
        try:
            ticker = self.client.futures_symbol_ticker(symbol=symbol)
            return float(ticker['price'])
        except Exception as e:
            print(f"Error getting price for {symbol}: {e}")
            return None

    # Binance上所有合约symbol的status
    def bi_get_all_contract_status(self):
        data = self.client.futures_exchange_info()
        symbols = data.get('symbols', [])
        rows = []
        for symbol_info in symbols:
            rows.append({
                'symbol': symbol_info['symbol'],
                'status': symbol_info['status']
            })
        df = pd.DataFrame(rows)

        return df


class GateDataHandler:

    def __init__(self, gate_key=None, gate_secret=None):

        self.config = Configuration(key=gate_key, secret=gate_secret)
        self.config.proxy = GATE_PROXY

        self.api_client = ApiClient(self.config)
        self.futures_api = FuturesApi(self.api_client)

    # Gateio所有合约实时资金费率
    def gate_get_funding_rates(self, symbol_filter="usdt"):

        contracts = self.futures_api.list_futures_contracts(settle=symbol_filter)
        df = pd.DataFrame([{
            'symbol': c.name,
            'mark_price': c.mark_price,
            'gate_funding_rate': c.funding_rate,
            'next_funding_time': c.funding_next_apply,
        } for c in contracts])

        df['gate_funding_rate'] = df['gate_funding_rate'].astype(float)
        df['mark_price'] = df['mark_price'].astype(float)
        df['symbol_renamed'] = df['symbol'].apply(lambda x: x.replace("_", ""))
        df['next_funding_time'] = pd.to_datetime(df['next_funding_time'], unit='s')

        df.sort_values(by="gate_funding_rate", ascending=False, inplace=True)

        return df

    # Gateio单个合约的实时funding rate
    def get_funding_rate(self, symbol):
        try:
            info = self.futures_api.get_futures_contract(settle='usdt', contract=symbol)
            return info.funding_rate
        except Exception as e:
            print(f"[Gate FR] 获取 {symbol} 资金费率失败: {e}")
            return 0.0001

class ArbitrageUtils:

    # 合并两个funding rate dataframe
    @staticmethod
    def merge_funding_rates(binance_df, gate_df):

        merged_df = pd.merge(
            left = gate_df[['symbol_renamed', 'mark_price', 'gate_funding_rate', 'next_funding_time']],
            right = binance_df[['symbol', 'markPrice','lastFundingRate', 'nextFundingTime']],
            left_on='symbol_renamed', right_on='symbol'
        )
        merged_df['fr_diff'] = merged_df['gate_funding_rate'] - merged_df['lastFundingRate']
        # merged_df['abs_price_diff'] = np.abs((merged_df['markPrice'] - merged_df['mark_price']) / merged_df['mark_price'])
        merged_df.sort_values(by='fr_diff', ascending=False, inplace=True)

        return merged_df

    # 合并两个funding rate interval dataframe
    @staticmethod
    def merge_funding_intervals(binance_df, gate_df):

        gate_df['symbol_renamed'] = gate_df['symbol'].apply(lambda x: x.replace("_", ""))
        merged_df = pd.merge(
            left=gate_df[['symbol_renamed', 'interval_hour']].rename(columns={'interval_hour': 'gate_interval'}),
            right=binance_df[['symbol', 'interval_hour']].rename(columns={'interval_hour': 'binance_interval'}),
            left_on='symbol_renamed',
            right_on='symbol',
            how='inner'
        )

        return merged_df

    # 获取最近的下一次funding生效时间
    @staticmethod
    def get_next_funding_time(merged_df):
        return min(merged_df['nextFundingTime'].min(), merged_df['next_funding_time'].min())

    # 最近的下一次funding生效时间，获取两边都有的币，并由此更新合并后的表
    @staticmethod
    def filter_next_funding_symbols(merged_df, next_funding_time):

        cond1 = merged_df['nextFundingTime'] == next_funding_time
        cond2 = merged_df['next_funding_time'] == next_funding_time

        return  merged_df[cond1&cond2]


if __name__ == '__main__':

    bdata_handler = BinanceDataHandler()
    gdata_handler = GateDataHandler()

    gate_df = gdata_handler.gate_get_funding_rates()
    print(gate_df.tail())
    print(gate_df.head())

    bi_df = bdata_handler.bi_get_funding_rates()
    print(bi_df.tail())
    print(bi_df.head())

    merge_df = ArbitrageUtils.merge_funding_rates(bi_df, gate_df)
    print(merge_df.head())
    print(merge_df.tail())

    # ob = bdata_handler.get_binance_orderbook('BTCUSDT')
    # print(ob)
    # print(ob['bids'][0][0])

    # print(ArbitrageUtils.adjust_price_to_tick(0.15723423532, 0.001))

    # info = bdata_handler.bi_get_contract_info('LPTUSDT')
    # tick = info['tick_size']
    # print(tick)
    #
    # price = 5.126
    # print(round(price/tick)*tick)

    # df = bdata_handler.bi_get_funding_rates()
    # print(df['status'].value_counts())
    # print(gdata_handler.get_funding_rate('ETH_USDT'))

    # bi_depth = bdata_handler.get_binance_orderbook(symbol='EDUUSDT')
    # print(bi_depth)
    #
    # g_depth = gdata_handler.get_gate_orderbook(symbol='EDUUSDT')
    # print(g_depth)
    #
    # pnl = ArbitrageUtils.calculate_worst_case_pnl(entry_price_gate=0.14109,
    #                                               entry_price_binance=0.1423,
    #                                               orderbook_gate=g_depth,
    #                                               orderbook_binance=bi_depth,
    #                                               trade_type='type1')
    # print(pnl)

    # ArbitrageUtils.update_interval_mismatch_list()

    # df = bdata_handler.bi_get_all_contract_status()
    # print(df['status'].value_counts())

    # s = 'BTCUSDT'
    # bi_status = bdata_handler.bi_get_contract_info(s)['status']
    # print(bi_status == 'TRADING')

    #
    # amount = 1000
    # symbol = 'BTCUSDT'
    #
    # gate_size, bi_quantity = ArbitrageUtils.calculate_trade_quantity(amount=20,
    #                                                                  symbol='ADAUSDT',
    #                                                                  binance_handler=bdata_handler,
    #                                                                  gate_handler=gdata_handler)
    # print(f"Gate Size: {gate_size}, Binance Quantity: {bi_quantity}")
    #
    # bi_df = bdata_handler.bi_get_funding_rates()
    # bi_df.to_csv('binance_fr.csv')
    # gate_df = gdata_handler.gate_get_funding_rates()
    # merged_df = ArbitrageUtils.merge_funding_rates(bi_df, gate_df)
    # # print(merged_df.info())
    # print(merged_df.tail())
    # print(merged_df.head())
    #
    # bi_interval = bdata_handler.bi_get_funding_interval_df()
    # gate_interval = gdata_handler.gate_get_funding_interval_df()
    #
    # interval_merged = ArbitrageUtils.merge_funding_intervals(bi_interval,gate_interval)
    # INTERVAL_MISMATCH_SYMBOLS = list(interval_merged[interval_merged['gate_interval'] != interval_merged['binance_interval']]['symbol'])
    # print(INTERVAL_MISMATCH_SYMBOLS)

    # interval_merged.to_csv('merged_funding_rate_interval.csv')
    #
    # fr_history = pd.DataFrame(bdata_handler.client.futures_funding_rate(symbol='FUNUSDT', limit=40))
    # fr_history['fundingTime'] = pd.to_datetime(fr_history['fundingTime'], unit='ms')
    # print(fr_history)

    #
    # nexttime = ArbitrageUtils.get_next_funding_time(merged_df)
    # print(nexttime)
    # #
    # filtered_df = ArbitrageUtils.filter_next_funding_symbols(merged_df, next_funding_time=nexttime)
    # print(filtered_df.info())
    # print(filtered_df.shape[0], filtered_df.shape[1])

    # print(gdata_handler.futures_api.get_futures_contract(settle='usdt', contract='BTC_USDT').funding_interval/3600)

    # print(ArbitrageUtils.calculate_price_diff(symbol='AERGOUSDT',
    #                                           binance_handler=bdata_handler,
    #                                           gate_handler=gdata_handler))

    # print(gdata_handler.gate_get_contract_info(contract_name='BTC_USDT'))
    #
    # print(gdata_handler.futures_api.list_futures_contracts(settle='usdt')[0])
