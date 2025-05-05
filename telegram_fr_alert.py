import asyncio
from telegram import Bot
import requests

from config import arbi_alarm, tele_chatid
from data import BinanceDataHandler, GateDataHandler, ArbitrageUtils

TELEGRAM_BOT_TOKEN = arbi_alarm
TELEGRAM_CHAT_ID = tele_chatid
bot = Bot(token=TELEGRAM_BOT_TOKEN)

def send_alert_sync(message: str, bot_token=TELEGRAM_BOT_TOKEN, chat_id=TELEGRAM_CHAT_ID):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown'
    }
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        print("✅ Message sent")
    except Exception as e:
        print(f"❌ Failed to send message: {e}")

# 获取资金费率相关信息
def get_funding_rate_summary():
    bdata_handler = BinanceDataHandler()
    gdata_handler = GateDataHandler()

    # 获取数据
    bi_df = bdata_handler.bi_get_funding_rates()
    gate_df = gdata_handler.gate_get_funding_rates()
    merged_df = ArbitrageUtils.merge_funding_rates(bi_df, gate_df)

    # 下次资金费率发放时间
    next_time = ArbitrageUtils.get_next_funding_time(merged_df)

    # 筛选下次资金费率发放的symbol
    filtered = ArbitrageUtils.filter_next_funding_symbols(merged_df=merged_df, next_funding_time=next_time)

    return {
        "next_funding_time": next_time,
        "bi_df": bi_df,
        "gate_df": gate_df,
        "filtered_df": filtered
    }

def format_funding_alert(summary, top_n=3) -> str:
    next_time = summary["next_funding_time"]

    gate_df = summary['gate_df'].copy()
    gate_filtered = gate_df[gate_df['next_funding_time']==next_time]

    gate_top = gate_filtered.sort_values("gate_funding_rate", ascending=False).head(top_n)
    gate_bottom = gate_filtered.sort_values("gate_funding_rate").head(top_n)

    bi_df = summary['bi_df'].copy()
    bi_filtered = bi_df[bi_df['nextFundingTime']==next_time]

    bi_top = bi_filtered.sort_values("lastFundingRate", ascending=False).head(top_n)
    bi_bottom = bi_filtered.sort_values("lastFundingRate").head(top_n)

    diff_df = summary["filtered_df"].copy()
    diff_top = diff_df.sort_values("fr_diff", ascending=False).head(top_n)
    diff_bottom = diff_df.sort_values("fr_diff").head(top_n)

    def df_to_text(df, label_col="symbol", value_col="fundingRate"):
        return "\n".join([
            f"{row[label_col]:<10} {float(row[value_col])*100:.4f}%" for _, row in df.iterrows()
        ])

    msg = (
        f"📊 下次资金费率发放时间：{next_time}\n\n"
        f"Gate：{gate_filtered.shape[0]} 个合约\n"
        f"Binance：{bi_filtered.shape[0]} 个合约\n"
        f"可比对合约（Gate ∩ Binance）：{diff_df.shape[0]} 个\n\n"
        f"📈 Gate 资金费率最高 Top {top_n}:\n{df_to_text(df=gate_top,value_col="gate_funding_rate")}\n\n"
        f"📉 Gate 资金费率最低 Top {top_n}:\n{df_to_text(gate_bottom,value_col="gate_funding_rate")}\n\n"
        f"📈 Binance 资金费率最高 Top {top_n}:\n{df_to_text(bi_top,value_col="lastFundingRate")}\n\n"
        f"📉 Binance 资金费率最低 Top {top_n}:\n{df_to_text(bi_bottom,value_col="lastFundingRate")}\n\n"
        f"🆚 Gate - Binance 差值最大 Top {top_n}:\n{df_to_text(diff_top, value_col='fr_diff')}\n\n"
        f"🆚 Gate - Binance 差值最小 Top {top_n}:\n{df_to_text(diff_bottom, value_col='fr_diff')}"
    )

    return msg

if __name__ == '__main__':
    pass
    # send_alert(message=f"test message at {datetime.datetime.now()}")
    # send_alert_sync(message='test2')
    #
    # results = get_funding_rate_summary()
    # print(results['next_funding_time'])
    # print(results['gate_df'].info())
    # print(results['gate_df'].head())
    # print(results['bi_df'].info())
    # print(results['bi_df'].head())
    # print(results['filtered_df'].info())
    # print(results['filtered_df'].head())
    # bi_df = results['bi_df'].copy()
    # bi_filtered = bi_df[bi_df['nextFundingTime'] == results['next_funding_time']]
    # print(bi_filtered.info())
    # print(bi_filtered.head())

    # msg = format_funding_alert(summary=results, top_n=3)
    # print(msg)
