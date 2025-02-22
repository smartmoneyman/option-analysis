import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import telebot

# === 1. Настройки ===
GOOGLE_DRIVE_FOLDER_NAME = "Опционы"
INPUT_FILE_NAME = "options_data.csv"
OUTPUT_FILE_NAME = "processed_options.csv"
TELEGRAM_BOT_TOKEN = "your_telegram_bot_token"
TELEGRAM_CHAT_ID = "your_chat_id"

# === 2. Подключение к Google Drive ===
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds_json = os.getenv("GDRIVE_CREDENTIALS")

if creds_json:
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
else:
    print("Ошибка: GDRIVE_CREDENTIALS не найден в переменных окружения.")
    exit()

# === 3. Поиск файла в Google Drive ===
drive_files = gc.list_spreadsheet_files()
file_id = None
for file in drive_files:
    if file['name'] == INPUT_FILE_NAME:
        file_id = file['id']
        break

if not file_id:
    print("Файл не найден в Google Drive.")
    exit()

# === 4. Скачивание и обработка файла ===
df = pd.read_csv(f"https://drive.google.com/uc?export=download&id={file_id}")

# Приведение данных к нужному формату
df['IV'] = df['IV'].str.replace('%', '').str.replace(',', '').astype(float)
df[['Bid', 'Ask', 'Last', 'Volume', 'Open Int']] = df[['Bid', 'Ask', 'Last', 'Volume', 'Open Int']].astype(float)
df['Strike'] = pd.to_numeric(df['Strike'], errors='coerce')
df['Price~'] = pd.to_numeric(df['Price~'], errors='coerce')
df['Exp Date'] = pd.to_datetime(df['Exp Date'], errors='coerce')

# Вычисление параметров
df['Days_to_Expiration'] = (df['Exp Date'] - datetime.today()).dt.days
df['Strike_Price_Diff'] = (df['Strike'] - df['Price~']).round(2)
df['Strike_Price_Diff_%'] = ((df['Strike_Price_Diff'] / df['Price~']) * 100).round(2)

# Группировка
option_analysis = df.groupby(['Symbol', 'Price~', 'Type', 'Strike', 'Exp Date']).agg(
    total_volume=('Volume', 'sum'),
    total_open_int=('Open Int', 'sum'),
    positive_delta_volume=('Volume', lambda x: (df.loc[x.index, 'Volume'] * df.loc[x.index, 'Delta']).clip(lower=0).sum()),
    negative_delta_volume=('Volume', lambda x: (-df.loc[x.index, 'Volume'] * df.loc[x.index, 'Delta']).clip(lower=0).sum()),
).reset_index()

# Добавление параметров
option_analysis['delta_volume_diff'] = option_analysis['positive_delta_volume'] - option_analysis['negative_delta_volume']
option_analysis['Days_to_Expiration'] = (option_analysis['Exp Date'] - datetime.today()).dt.days
option_analysis['Strike_Price_Diff'] = (option_analysis['Strike'] - option_analysis['Price~']).round(2)
option_analysis['Strike_Price_Diff_%'] = ((option_analysis['Strike_Price_Diff'] / option_analysis['Price~']) * 100).round(2)

# Фильтрация по объему и дельте
filtered_data = option_analysis[
    (option_analysis['total_volume'] >= 1000) & (option_analysis['delta_volume_diff'] > 0)
]

# === 5. Сохранение файла в Google Drive ===
output_path = f"/content/drive/My Drive/{GOOGLE_DRIVE_FOLDER_NAME}/{OUTPUT_FILE_NAME}"
filtered_data.to_csv(output_path, index=False)
print(f"Файл сохранен в Google Drive: {output_path}")

# === 6. Отправка файла в Telegram ===
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
with open(output_path, "rb") as doc:
    bot.send_document(TELEGRAM_CHAT_ID, doc, caption="Обработанный файл с опционами")
print("Файл отправлен в Telegram.")
