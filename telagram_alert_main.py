import time
from datetime import datetime, timedelta, timezone

from telegram_fr_alert import get_funding_rate_summary, format_funding_alert, send_alert_sync

# 每轮资金费率只提醒一次
last_alert_time = None

def funding_alert_loop():
    global last_alert_time
    print("📡 开始资金费率提醒循环...")

    while True:
        try:
            summary = get_funding_rate_summary()
            next_time = summary['next_funding_time']
            if next_time.tzinfo is None:
                next_time = next_time.replace(tzinfo=timezone.utc)

            now = datetime.now(timezone.utc)

            # 如果现在处于提醒窗口，并且还没提醒过这个时间点
            if abs((next_time - now).total_seconds()) < 1800:
                if last_alert_time != next_time:
                    msg = format_funding_alert(summary)
                    send_alert_sync(msg)
                    last_alert_time = next_time
                    print(f"✅ {now} 成功发送资金费率提醒")
                else:
                    print(f"[SKIP] 已提醒过 {next_time}，不重复提醒")
            else:
                print(f"[WAIT] 当前不在提醒窗口内。Now: {now}, Next FR Time: {next_time}")

        except Exception as e:
            print(f"❌ 出现错误: {e}")

        time.sleep(300)

if __name__ == "__main__":
    funding_alert_loop()