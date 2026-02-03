import gspread
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
import pandas as pd
from typing import List, Dict, Optional
import logging
from datetime import datetime

class GoogleSheetsClient:
    def __init__(self, credentials_path: str = None, service_account_path: str = None):
        self.logger = logging.getLogger(__name__)
        
        if service_account_path:
            # Используем Service Account
            self.creds = Credentials.from_service_account_file(
                service_account_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
        elif credentials_path:
            # Используем OAuth 2.0
            self.scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            self.creds = None
            self.credentials_path = credentials_path
            self._authenticate()
        else:
            raise ValueError("Необходимо указать либо credentials_path, либо service_account_path")
            
        self.client = gspread.authorize(self.creds)

    def _authenticate(self):
        """Аутентификация через OAuth 2.0"""
        try:
            self.creds = Credentials.from_authorized_user_file(self.credentials_path, self.scopes)
        except FileNotFoundError:
            flow = InstalledAppFlow.from_client_secrets_file(self.credentials_path, self.scopes)
            self.creds = flow.run_local_server(port=0)
            
            # Сохраняем credentials для будущего использования
            with open('token.json', 'w') as token:
                token.write(self.creds.to_json())

    def create_spreadsheet(self, title: str) -> str:
        """Создает новую Google Таблицу"""
        try:
            spreadsheet = self.client.create(title)
            self.logger.info(f"Создана таблица: {spreadsheet.title} (ID: {spreadsheet.id})")
            return spreadsheet.id
        except Exception as e:
            self.logger.error(f"Ошибка при создании таблицы: {str(e)}")
            raise

    def get_or_create_spreadsheet(self, title: str) -> str:
        """Получает существующую таблицу или создает новую"""
        try:
            # Ищем существующую таблицу
            spreadsheets = self.client.list_spreadsheet_files()
            for sp in spreadsheets:
                if sp['name'] == title:
                    self.logger.info(f"Найдена существующая таблица: {title}")
                    return sp['id']
            
            # Если не нашли, создаем новую
            return self.create_spreadsheet(title)
        except Exception as e:
            self.logger.error(f"Ошибка при поиске/создании таблицы: {str(e)}")
            raise

    def write_data_to_sheet(self, spreadsheet_id: str, data: pd.DataFrame, worksheet_name: str = "Сообщения") -> bool:
        """Записывает данные в лист таблицы"""
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            
            # Удаляем старый лист если он существует
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                spreadsheet.del_worksheet(worksheet)
            except gspread.WorksheetNotFound:
                pass
            
            # Создаем новый лист
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=len(data) + 1, cols=len(data.columns))
            
            # Записываем данные
            worksheet.update([data.columns.values.tolist()] + data.values.tolist())
            
            # Форматируем заголовки
            worksheet.format('A1:' + chr(ord('A') + len(data.columns) - 1) + '1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            
            # Автоматическая ширина колонок
            for i, col in enumerate(data.columns):
                worksheet.format(f'{chr(ord("A") + i)}:{chr(ord("A") + i)}', {
                    'wrapStrategy': 'WRAP'
                })
            
            self.logger.info(f"Данные успешно записаны в лист '{worksheet_name}'")
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка при записи данных: {str(e)}")
            return False

    def write_summary_to_sheet(self, spreadsheet_id: str, summary: Dict, worksheet_name: str = "Сводка") -> bool:
        """Записывает сводную статистику в отдельный лист"""
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            
            # Удаляем старый лист если он существует
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                spreadsheet.del_worksheet(worksheet)
            except gspread.WorksheetNotFound:
                pass
            
            # Создаем новый лист
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=50, cols=3)
            
            # Подготавливаем данные для записи
            summary_data = []
            summary_data.append(["Метрика", "Значение", ""])
            summary_data.append(["", "", ""])  # Пустая строка
            
            for key, value in summary.items():
                if isinstance(value, dict):
                    summary_data.append([key, "", ""])
                    for sub_key, sub_value in value.items():
                        summary_data.append([f"  {sub_key}", str(sub_value), ""])
                    summary_data.append(["", "", ""])
                else:
                    summary_data.append([key, str(value), ""])
            
            # Добавляем информацию о времени обновления
            summary_data.append(["", "", ""])
            summary_data.append(["Последнее обновление", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), ""])
            
            # Записываем данные
            worksheet.update(summary_data)
            
            # Форматируем
            worksheet.format('A1:B1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            
            self.logger.info(f"Сводка успешно записана в лист '{worksheet_name}'")
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка при записи сводки: {str(e)}")
            return False

    def append_data_to_sheet(self, spreadsheet_id: str, data: pd.DataFrame, worksheet_name: str = "Сообщения") -> bool:
        """Добавляет новые данные в конец существующего листа"""
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
            except gspread.WorksheetNotFound:
                # Если лист не существует, создаем его
                return self.write_data_to_sheet(spreadsheet_id, data, worksheet_name)
            
            # Добавляем данные в конец
            worksheet.append_rows(data.values.tolist())
            
            self.logger.info(f"Добавлено {len(data)} новых сообщений в лист '{worksheet_name}'")
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка при добавлении данных: {str(e)}")
            return False

    def get_spreadsheet_url(self, spreadsheet_id: str) -> str:
        """Получает URL таблицы"""
        return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
