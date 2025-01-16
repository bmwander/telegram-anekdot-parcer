import telethon #Модуль для работы с tg
from telethon.sync import TelegramClient
import csv
from datetime import datetime #Модуль даты
import re #Модуль для замены в тексте
import pandas as pd #Модули для графиков
import plotly.express as px

# ЧАСТЬ 1. НАСТРОЙКИ ДЛЯ КОДА
# Данные для авторизации (полученные на сайте https://my.telegram.org/apps)
api_id = '25706405'
api_hash = 'fdfd5c7345ae116029ba18b1a1f605c4'
phone = '+79218559043' #Ваш номер телефона в tg

# Список групп
groups = ['banekdot', 'anekdots']

# Количество сообщений (максимальное), которое нужно спарсить из каждой группы
message_limit = 5000

# Запускаем клиент для работы с tg
client = TelegramClient('session_name', api_id, api_hash)

# ЧАСТЬ 2. ОСНОВНЫЕ ФУНКЦИИ
# Функция для фильтрации текста (от рекламы и ссылок)
def filter_text(text):
    if '*' in text or 'https://t.me/' in text:
        return False
    return True

# Функция для удаления упоминаний группы из текста (обычно они в конце, поэтому обычной замены на пустую строку достаточно)
def remove(text):
    return re.sub(r'@\w+', '', text).strip()

#Функция для парсинга сообщений
async def parsing_messages(): #Используем ассинхронную функцию, т.к. нам надо создавать отдельную "сессию" для работы с tg, и функции telethon работают только в этом режиме 
    await client.start(phone) #Запускаем tg-клиент
    with open('all_messages.csv', 'w', newline='', encoding='utf-8') as file: #Создаем csv-файл с сообщениями
        writer = csv.writer(file)
        writer.writerow(['Group Name', 'Message ID', 'Date', 'Text'])
        for group in groups:
            try:
                channel = await client.get_entity(group) #Получаем доступ к группе из списка
                async for message in client.iter_messages(channel, limit = message_limit): #Просматриваем все последние N (=message_limit) сообщений
                    if message.text and filter_text(message.text): #Проверяем на все условия (чтобы был только текст, без картинок и ненужных символов) и записываем результат
                        cleaned_text = remove(message.text)
                        writer.writerow([channel.title, message.id, message.date.strftime('%Y-%m-%d'), cleaned_text])
                print(f"Группа '{group}' обработана.")
            except Exception as error: #Выводим сообщение об ошибке при обработке группы
                print(f"Ошибка при обработке группы '{group}': {error}") 

#Запускаем Парсер
with client:
    client.loop.run_until_complete(parsing_messages())

# ЧАСТЬ 3. АНАЛИЗ ДАННЫХ
# Находим ключевые слова, которые будем анализировать
keywords = ["Вовочка", "теща", "Ржевский", "студент", "еврей", "русский", "грузин", "армянин"]
keywords_in = []

#"Открываем" нащ csv.файл и добавляем в список keywords_in соответствующие строки
with open('all_messages.csv', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        group_name = row['Group Name']
        date = row['Date']
        text = row['Text']
        for keyword in keywords:
            if keyword in text:
                keywords_in.append({'Group Name': group_name, 'Date': date, 'Keyword': keyword})

# Создаем датафрейм из данных
keywords_df = pd.DataFrame(keywords_in)

# Построение общей гистограммы по ключевым словам для всех групп
keyword_hist = px.histogram(
    keywords_df,
    x='Keyword',
    color='Group Name',
    barmode='group',
    title='Распределение анекдотов по ключевым словам',
    labels={'Keyword': 'Ключевое слово', 'count': 'Количество анекдотов', 'Group Name': 'Источник'}
)
keyword_hist.show()

# Считываем данные из файла и строим график по датам для каждой группы
original_data = pd.read_csv('all_messages.csv')
for group in original_data['Group Name'].unique():
    group_data = original_data[original_data['Group Name'] == group]
    date_plot = px.line(
        group_data.groupby('Date').size().reset_index(name='count'),
        x='Date',
        y='count',
        title=f'Количество анекдотов по датам для канала {group}',
        labels={'Date': 'Дата', 'count': 'Количество анекдотов'}
    )
    date_plot.show()

