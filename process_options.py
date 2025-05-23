import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import telebot
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io

# === 1. Дешифровка service_account.json ===
if not os.path.exists("service_account.json"):
    print("❌ Файл service_account.json не найден. Проверь дешифровку!")
    exit(1)

print("✅ Найден service_account.json, продолжаем...")

# === 2. Подключение к Google Drive ===
SCOPES = ['https://www.googleapis.com/auth/drive']
with open("service_account.json", "r") as f:
    creds_dict = json.load(f)

creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
gc = gspread.authorize(creds)
drive_service = build('drive', 'v3', credentials=creds)

# === 3. Параметры ===
FOLDER_ID = "1J1W5nmnTJWzgruO-zccypP4IddD_JjTU"
OUTPUT_FILE_NAME = "processed_options.csv"

# Telegram Bot
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not TELEGRAM_BOT_TOKEN:
    print("❌ Ошибка: TELEGRAM_BOT_TOKEN не найден в переменных окружения!")
    exit(1)
if not TELEGRAM_CHAT_ID:
    print("❌ Ошибка: TELEGRAM_CHAT_ID не найден в переменных окружения!")
    exit(1)

bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
print("✅ Telegram Bot успешно инициализирован")

# === Найти последний загруженный файл ===
try:
    query = f"'{FOLDER_ID}' in parents"
    response = drive_service.files().list(
        q=query,
        fields="files(id, name, createdTime)",
        orderBy="createdTime desc",
        pageSize=1
    ).execute()

    files = response.get('files', [])

    if not files:
        print("❌ В папке Google Drive нет файлов!")
        exit(1)

    latest_file = files[0]
    file_id = latest_file['id']
    file_name = latest_file['name']
    print(f"✅ Найден последний загруженный файл: {file_name} (ID: {file_id})")

except Exception as e:
    print(f"❌ Ошибка при поиске файла в Google Drive: {e}")
    exit(1)

# === Скачать файл ===
try:
    request = drive_service.files().get_media(fileId=file_id)
    file_stream = io.BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    file_stream.seek(0)  # Перемещаем указатель в начало файла
    print(f"✅ Файл {file_name} загружен и готов к обработке.")

    # === Преобразуем загруженный файл в DataFrame ===
    df = pd.read_csv(file_stream)
    print("✅ Файл загружен в DataFrame, начинаем обработку.")

except Exception as e:
    print(f"❌ Ошибка при скачивании файла: {e}")
    exit(1)

# === 6. Обработка данных ===
df['IV'] = df['IV'].str.replace('%', '').str.replace(',', '').astype(float)
df[['Bid', 'Ask', 'Last', 'Volume', 'Open Int']] = df[['Bid', 'Ask', 'Last', 'Volume', 'Open Int']].astype(float)
df['Strike'] = pd.to_numeric(df['Strike'], errors='coerce')
df['Price~'] = pd.to_numeric(df['Price~'], errors='coerce')
df['Exp Date'] = pd.to_datetime(df['Exp Date'], errors='coerce')

# Вычисление параметров
df['Days_to_Expiration'] = (df['Exp Date'] - datetime.today()).dt.days
df['Strike_Price_Diff'] = (df['Strike'] - df['Price~']).round(2)
df['Strike_Price_Diff_%'] = ((df['Strike_Price_Diff'] / df['Price~']) * 100).round(2)

# === 7. Сохранение данных ===
output_file_path = "/tmp/processed_options.csv"

if not df.empty:
    df.to_csv(output_file_path, index=False)
    print(f"✅ Файл обработан и сохранен: {output_file_path}")
else:
    print("⚠️ Нет данных для обработки.")

# === 8. Загрузка обработанного файла в Google Drive ===
try:
    file_metadata = {
        "name": OUTPUT_FILE_NAME,
        "parents": [FOLDER_ID]
    }
    media = MediaFileUpload(output_file_path, mimetype="text/csv")
    uploaded_file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()

    if uploaded_file and "id" in uploaded_file:
        print(f"✅ Файл загружен в Google Drive: {uploaded_file.get('id')}")
    else:
        print("❌ Ошибка: Файл не загружен в Google Drive.")

except Exception as e:
    print(f"❌ Ошибка при загрузке файла в Google Drive: {e}")

# === 9. Отправка файла в Telegram ===
try:
    with open(output_file_path, "rb") as doc:
        bot.send_document(TELEGRAM_CHAT_ID, doc, caption="📊 Обработанный файл с опционами")
    print("✅ Файл отправлен в Telegram.")
except Exception as e:
    print(f"❌ Ошибка при отправке файла в Telegram: {e}")
