import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import telebot
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# === Параметры ===
FOLDER_ID = "1J1W5nmnTJWzgruO-zccypP4IddD_JjTU"  # ID папки в Google Drive
INPUT_FILE_NAME = "options_data.csv"
OUTPUT_FILE_NAME = "processed_options.csv"

# Telegram Bot
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# === 1. Подключение к Google Drive ===
SCOPES = ['https://www.googleapis.com/auth/drive']
creds_json = os.getenv("GDRIVE_CREDENTIALS")

if creds_json:
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    drive_service = build('drive', 'v3', credentials=creds)
else:
    print("Ошибка: GDRIVE_CREDENTIALS не найден в переменных окружения.")
    exit()

# === 2. Поиск файла в Google Drive ===
query = f"name='{INPUT_FILE_NAME}' and '{FOLDER_ID}' in parents"
response = drive_service.files().list(q=query, fields="files(id, name)").execute()
files = response.get('files', [])

if not files:
    print("Файл не найден в Google Drive.")
    exit()

file_id = files[0]['id']

# === 3. Скачивание файла ===
request = drive_service.files().get_media(fileId=file_id)
file_stream = io.BytesIO()
downloader = MediaIoBaseDownload(file_stream, request)
done = False
while not done:
    _, done = downloader.next_chunk()

file_stream.seek(0)
df = pd.read_csv(file_stream)

# === 4. Обработка данных ===
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

# === 5. Сохранение обработанного файла ===
output_file_path = f"/tmp/{OUTPUT_FILE_NAME}"
filtered_data.to_csv(output_file_path, index=False)

# === 6. Загрузка файла обратно в Google Drive ===
file_metadata = {
    "name": OUTPUT_FILE_NAME,
    "parents": [FOLDER_ID]
}
media = drive_service.files().create(body=file_metadata, media_body=output_file_path, fields="id").execute()
print(f"Файл загружен в Google Drive: {media.get('id')}")

# === 7. Отправка файла в Telegram ===
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
with open(output_file_path, "rb") as doc:
    bot.send_document(TELEGRAM_CHAT_ID, doc, caption="Обработанный файл с опционами")
print("Файл отправлен в Telegram.")
