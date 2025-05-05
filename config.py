from dotenv import load_dotenv
import os

# load proxies
load_dotenv('proxy.env')
BINANCE_PROXY = os.getenv('BINANCE_PROXY')
GATE_PROXY = os.getenv('GATE_PROXY')

# load Telegram bot token
load_dotenv('telegram_bot.env')
arbi_alarm = os.getenv("ArbiAlarmBot")
tele_chatid = os.getenv("chat_id")
