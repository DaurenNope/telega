import logging
import os
from datetime import datetime, timezone # Or however you get timestamps
from dotenv import load_dotenv

# --- Force DEBUG level for root logger ---
logging.getLogger().setLevel(logging.DEBUG)

# --- Load Environment Variables ---
# Assumes .env is in the same directory or parent directory
# Adjust path if needed, e.g., os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv()

# --- Import after loading env vars ---
try:
    # Only import the necessary public functions
    from src.analyzer import init_analyzer, extract_message_data
except ImportError as e:
    print(f"ImportError: {e}. Ensure you are running this from the workspace root and src/analyzer.py exists.")
    exit(1)


# Configure logging
console = logging.getLogger() # Use standard logger for simplicity here

# --- Hardcode the test data ---
test_message_link = "https://t.me/c/1520572254/3922"
test_channel = "Crypto Drive" # Assuming based on previous query results
# Using the actual message text from the previous query
test_message_text = """
✅**Optimism:**[ ](https://img1.teletype.in/files/40/68/40687c44-6a55-4507-a68a-ccb2c474fae5.jpeg)**Люто выносим шестой AirDrop**

Ранее неоднократно [**упоминал**](https://t.me/drivecrypto/3832) проект в подборках и делал [**подробные**](https://t.me/drivecrypto/3621)[** **](https://t.me/drivecrypto/3621)[**посты**](https://t.me/drivecrypto/3621). Если забыли, то всего было **5** дропов, причем нормальных. Команда планирует раздать еще **13%** токенов $OP.

Сейчас решил сделать список активностей и поделится мыслями. Пройдемся по порядку.

**ℹ️**** SuperStack**
Команда проекта запустила кампанию [**SuperStack**](https://app.optimism.io/superstacks) буквально сегодня. В ее рамках мы сможем выполнять задания и получать **XP** (за предоставление ликвидности в протоколы).

— Переходим на [**сайт**](https://app.optimism.io/superstacks) и подключаем кошелек
— Листаем ниже, выбираем понравившийся пул

Кампания будет актуальна до **30** **июня**. Сейчас доступно **6** **сетей** __(OP, INK, Unichain, Base, Soneium, World)__. Доступно **2** даппки: **UniSwap** и **Aerodrome**. Дают по **10** **XP** за каждый **$.**

**ℹ️**** SuperBadges**
Мы можем создать свой \"**Super Account**\"__ (аналогично **ENS** домена)__. Есть возможность кастомизации + в аккаунте есть **leaderboard** и задания с бейджами.

— На [**сайте**](https://account.superchain.eco/) подключаем свой кошелек
— Придумываем себе ник & имя
— Создаем аватар и фармим [**бейджи**](https://account.superchain.eco/badges)

Если вы ранее активничали, можете нажать на \"**CLAIM BADGES**\" (тем самым проверите какие бейджи у вас есть). + затрагиваю другие проекты, типу **Unichain**, **Soneium**, **INK**

Также не забывайте про **Superboard Quests**. Это всеми известная платформа с квестами от разных чейнов (из экосистемы **Superchain**). Много говорить не буду, делайте квесты на [**сайте**](https://superboard.xyz/quests)

Затраты у нас минимальные__ (все таки L2)__. Если у вас есть токены $OP, и вы их не делегировали — [**исправляйтесь**](https://vote.optimism.io/delegates) (сможете брать участие в голосованиях), за это ранее был дроп

🐳 — Насыпка будет вкусной, work
👾 — Шляпа все это, скип __(напишу мысли в комментах)__

💎 Наш [чат по дропам:](https://t.me/+l3JdEnmDKAU3OGFi) стратегии по выносу, взаимопомощь и польза, ждем всех!
**\nВсе для мультиакккинга:** ✔[LINK](https://t.me/drivecrypto/3578)         
**CryptoDrive:** ✉️[Telegram ](https://t.me/drivecrypto)| ▶️[Youtube](https://www.youtube.com/@crypto_drive)
"""
# Use a fixed timestamp for testing consistency, make it timezone-aware (UTC)
# You can replace this with the actual message timestamp if needed
test_timestamp = datetime(2025, 4, 17, 16, 2, 2, tzinfo=timezone.utc)

# --- Run the analysis ---
if __name__ == "__main__":
    if init_analyzer():
        console.info(f"Running analysis for test message: {test_message_link}")
        try:
            # Run the standard analysis function
            count, guide_flag, error = extract_message_data(
                test_message_text,
                test_channel,
                test_timestamp,
                test_message_link
            )
            console.info("--- Analysis Results ---")
            console.info(f"Updates Saved Count: {count}")
            console.info(f"Guide Saved Flag: {guide_flag}")
            console.info(f"Error Message: {error}")
            console.info("----------------------")
            if error:
                console.warning("Analysis returned an error.")
            elif count == 0 and not guide_flag:
                console.warning("Analysis completed, but no relevant updates or guide were identified/saved.")
            else:
                 console.info("Analysis completed. Please check Supabase for the saved data.")

        except Exception as e:
            console.error(f"CRITICAL ERROR during test analysis run: {e}", exc_info=True)
    else:
        console.error("Failed to initialize analyzer. Check logs and .env configuration.") 