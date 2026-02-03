#!/usr/bin/env python3
"""
WhatsApp AI Agent для сбора и анализа сообщений из групп
"""

import os
import logging
import schedule
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from whatsapp_client import WhatsAppClient
from data_processor import DataProcessor
from google_sheets_client import GoogleSheetsClient

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('whatsapp_agent.log'),
        logging.StreamHandler()
    ]
)

class WhatsAppAgent:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Инициализация клиентов
        self.whatsapp_client = WhatsAppClient(
            api_token=os.getenv('WHATSAPP_API_TOKEN'),
            phone_number_id=os.getenv('WHATSAPP_PHONE_NUMBER_ID')
        )
        
        self.data_processor = DataProcessor(
            openai_api_key=os.getenv('OPENAI_API_KEY')
        )
        
        self.google_sheets_client = GoogleSheetsClient(
            service_account_path=os.getenv('GOOGLE_SERVICE_ACCOUNT_PATH'),
            credentials_path=os.getenv('GOOGLE_CREDENTIALS_PATH')
        )
        
        # Настройки из переменных окружения
        self.spreadsheet_title = os.getenv('SPREADSHEET_TITLE', 'WhatsApp Анализ Сообщений')
        self.group_ids = os.getenv('WHATSAPP_GROUP_IDS', '').split(',') if os.getenv('WHATSAPP_GROUP_IDS') else []
        self.messages_per_group = int(os.getenv('MESSAGES_PER_GROUP', '50'))
        self.update_interval_minutes = int(os.getenv('UPDATE_INTERVAL_MINUTES', '30'))
        
        # ID таблицы Google Sheets
        self.spreadsheet_id = None

    def setup_spreadsheet(self):
        """Настраивает Google Таблицу"""
        try:
            self.spreadsheet_id = self.google_sheets_client.get_or_create_spreadsheet(self.spreadsheet_title)
            self.logger.info(f"Google Таблица готова: {self.google_sheets_client.get_spreadsheet_url(self.spreadsheet_id)}")
            return True
        except Exception as e:
            self.logger.error(f"Ошибка настройки таблицы: {str(e)}")
            return False

    def collect_and_process_messages(self):
        """Основной цикл сбора и обработки сообщений"""
        try:
            self.logger.info("Начинаю сбор сообщений из WhatsApp групп...")
            
            # Собираем сообщения
            all_messages = self.whatsapp_client.get_messages_from_multiple_groups(
                self.group_ids, 
                self.messages_per_group
            )
            
            if not all_messages:
                self.logger.warning("Не получено ни одного сообщения")
                return False
            
            self.logger.info(f"Собрано {len(all_messages)} сообщений")
            
            # Обрабатываем данные
            processed_df = self.data_processor.process_messages(all_messages)
            
            # Генерируем сводку
            summary = self.data_processor.generate_summary(processed_df)
            
            # Записываем в Google Sheets
            if self.spreadsheet_id:
                # Обновляем основной лист с сообщениями
                self.google_sheets_client.write_data_to_sheet(
                    self.spreadsheet_id, 
                    processed_df, 
                    "Сообщения"
                )
                
                # Обновляем лист со сводкой
                self.google_sheets_client.write_summary_to_sheet(
                    self.spreadsheet_id, 
                    summary, 
                    "Сводка"
                )
                
                self.logger.info(f"Данные успешно обновлены в Google Таблице")
                
                # Выводим статистику
                self.logger.info(f"Статистика: {summary.get('Всего сообщений', 0)} сообщений, {summary.get('Уникальных авторов', 0)} авторов")
                
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка при сборе и обработке сообщений: {str(e)}")
            return False

    def get_new_messages_only(self):
        """Получает только новые сообщения с последнего запуска"""
        try:
            # Здесь можно реализовать логику получения только новых сообщений
            # Например, сохранять timestamp последнего сообщения и запрашивать только более новые
            
            last_run_file = 'last_run.txt'
            last_timestamp = None
            
            if os.path.exists(last_run_file):
                with open(last_run_file, 'r') as f:
                    last_timestamp = f.read().strip()
            
            # Собираем сообщения
            all_messages = self.whatsapp_client.get_messages_from_multiple_groups(
                self.group_ids, 
                self.messages_per_group
            )
            
            # Фильтруем только новые сообщения
            if last_timestamp:
                new_messages = [msg for msg in all_messages if msg.get('timestamp', '') > last_timestamp]
            else:
                new_messages = all_messages
            
            if new_messages:
                # Обрабатываем новые сообщения
                processed_df = self.data_processor.process_messages(new_messages)
                
                # Добавляем в таблицу
                if self.spreadsheet_id:
                    self.google_sheets_client.append_data_to_sheet(
                        self.spreadsheet_id, 
                        processed_df, 
                        "Сообщения"
                    )
                
                self.logger.info(f"Добавлено {len(new_messages)} новых сообщений")
            
            # Сохраняем timestamp последнего запуска
            with open(last_run_file, 'w') as f:
                f.write(datetime.now().isoformat())
                
        except Exception as e:
            self.logger.error(f"Ошибка при получении новых сообщений: {str(e)}")

    def run_once(self):
        """Запускает агент один раз"""
        self.logger.info("Запуск WhatsApp агента (однократный режим)")
        
        if not self.setup_spreadsheet():
            return False
            
        return self.collect_and_process_messages()

    def run_scheduled(self):
        """Запускает агента по расписанию"""
        self.logger.info("Запуск WhatsApp агента (режим по расписанию)")
        
        if not self.setup_spreadsheet():
            return
        
        # Первоначальный запуск
        self.collect_and_process_messages()
        
        # Настраиваем расписание
        schedule.every(self.update_interval_minutes).minutes.do(self.get_new_messages_only)
        
        self.logger.info(f"Агент запущен. Обновление каждые {self.update_interval_minutes} минут")
        
        while True:
            schedule.run_pending()
            time.sleep(60)  # Проверяем каждую минуту

    def test_connection(self):
        """Тестирует подключение к сервисам"""
        self.logger.info("Тестирование подключений...")
        
        try:
            # Тест WhatsApp API
            groups = self.whatsapp_client.get_groups_list()
            self.logger.info(f"WhatsApp API: найдено {len(groups)} групп")
            
            # Тест Google Sheets
            if self.setup_spreadsheet():
                self.logger.info("Google Sheets: подключение успешно")
            else:
                self.logger.error("Google Sheets: ошибка подключения")
                
        except Exception as e:
            self.logger.error(f"Ошибка тестирования: {str(e)}")

def main():
    """Основная функция"""
    agent = WhatsAppAgent()
    
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "test":
            agent.test_connection()
        elif command == "run":
            agent.run_once()
        elif command == "schedule":
            agent.run_scheduled()
        else:
            print("Использование: python whatsapp_agent.py [test|run|schedule]")
    else:
        # По умолчанию запускаем один раз
        agent.run_once()

if __name__ == "__main__":
    main()
