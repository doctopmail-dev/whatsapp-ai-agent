import requests
import json
import time
from datetime import datetime
from typing import List, Dict, Optional
import logging

class WhatsAppClient:
    def __init__(self, api_token: str, phone_number_id: str):
        self.api_token = api_token
        self.phone_number_id = phone_number_id
        self.base_url = "https://graph.facebook.com/v18.0"
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }
        self.logger = logging.getLogger(__name__)

    def get_group_messages(self, group_id: str, limit: int = 100) -> List[Dict]:
        """Получает сообщения из WhatsApp группы"""
        messages = []
        
        try:
            url = f"{self.base_url}/{group_id}/messages"
            params = {
                "limit": limit,
                "fields": "id,from,to,text,timestamp,author,type"
            }
            
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                messages = data.get("data", [])
                self.logger.info(f"Получено {len(messages)} сообщений из группы {group_id}")
            else:
                self.logger.error(f"Ошибка при получении сообщений: {response.status_code} - {response.text}")
                
        except Exception as e:
            self.logger.error(f"Исключение при получении сообщений: {str(e)}")
            
        return messages

    def get_groups_list(self) -> List[Dict]:
        """Получает список групп WhatsApp"""
        groups = []
        
        try:
            url = f"{self.base_url}/me/conversations"
            params = {
                "fields": "id,name,participants,is_group",
                "limit": 50
            }
            
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                conversations = data.get("data", [])
                groups = [conv for conv in conversations if conv.get("is_group", False)]
                self.logger.info(f"Найдено {len(groups)} групп")
            else:
                self.logger.error(f"Ошибка при получении групп: {response.status_code} - {response.text}")
                
        except Exception as e:
            self.logger.error(f"Исключение при получении групп: {str(e)}")
            
        return groups

    def process_message(self, message: Dict) -> Dict:
        """Обрабатывает и структурирует сообщение"""
        processed = {
            "message_id": message.get("id"),
            "author": message.get("author", "Unknown"),
            "text": message.get("text", {}).get("body", ""),
            "timestamp": datetime.fromtimestamp(int(message.get("timestamp", 0))).isoformat(),
            "type": message.get("type", "text"),
            "from_phone": message.get("from"),
            "to_phone": message.get("to")
        }
        
        return processed

    def get_messages_from_multiple_groups(self, group_ids: List[str], messages_per_group: int = 50) -> List[Dict]:
        """Собирает сообщения из нескольких групп"""
        all_messages = []
        
        for group_id in group_ids:
            try:
                messages = self.get_group_messages(group_id, messages_per_group)
                processed_messages = [self.process_message(msg) for msg in messages]
                all_messages.extend(processed_messages)
                
                # Небольшая задержка между запросами
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Ошибка при обработке группы {group_id}: {str(e)}")
                continue
                
        # Сортируем по времени
        all_messages.sort(key=lambda x: x["timestamp"], reverse=True)
        
        return all_messages
