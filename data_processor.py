import re
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
import logging
from openai import OpenAI

class DataProcessor:
    def __init__(self, openai_api_key: Optional[str] = None):
        self.openai_client = OpenAI(api_key=openai_api_key) if openai_api_key else None
        self.logger = logging.getLogger(__name__)

    def clean_text(self, text: str) -> str:
        """Очищает текст от лишних символов и форматирования"""
        if not text:
            return ""
            
        # Удаляем лишние пробелы и переносы строк
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Удаляем специальные символы WhatsApp
        text = re.sub(r'[^\w\s\.\,\!\?\-\:\;\(\)\[\]\{\}\"\'\/\@\#\$\%\^\&\*\+\=\~\`]', '', text)
        
        return text

    def extract_keywords(self, text: str) -> List[str]:
        """Извлекает ключевые слова из текста"""
        # Простое извлечение ключевых слов
        words = re.findall(r'\b[a-zA-Zа-яА-Я]{3,}\b', text.lower())
        
        # Фильтруем стоп-слова
        stop_words = {'это', 'как', 'что', 'когда', 'где', 'почему', 'который', 'для', 'с', 'на', 'и', 'в', 'не', 'по', 'к', 'у', 'от', 'до', 'о', 'об', 'при', 'за', 'так', 'же', 'быть', 'был', 'была', 'было', 'будет', 'есть', 'are', 'the', 'is', 'at', 'which', 'on', 'and', 'or', 'but', 'in', 'with', 'to', 'for', 'of', 'as', 'by', 'that', 'this', 'it', 'from'}
        
        keywords = [word for word in words if word not in stop_words and len(word) > 2]
        
        return list(set(keywords[:10]))  # Возвращаем уникальные ключевые слова

    def categorize_message(self, text: str) -> str:
        """Категоризирует сообщение по содержанию"""
        text_lower = text.lower()
        
        categories = {
            "Продажи/Реклама": ["купить", "продать", "цена", "скидка", "акция", "предложение", "buy", "sell", "price", "discount"],
            "Вопросы": ["?", "как", "почему", "когда", "где", "что", "кто", "why", "when", "where", "what", "who"],
            "Информация/Новости": ["новость", "информация", "обновление", "важно", "news", "information", "update", "important"],
            "Общение/Чат": ["привет", "здравствуйте", "спасибо", "пока", "hello", "hi", "thanks", "bye"],
            "Ссылки/Медиа": ["http", "www", ".com", ".ru", "ссылка", "link", "video", "photo"],
            "Работа/Бизнес": ["работа", "проект", "задача", "дедлайн", "work", "project", "task", "deadline"],
            "Другое": []
        }
        
        for category, keywords in categories.items():
            if any(keyword in text_lower for keyword in keywords):
                return category
                
        return "Другое"

    def analyze_sentiment(self, text: str) -> str:
        """Анализирует тональность сообщения"""
        if not self.openai_client:
            return "Не определено"
            
        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "Определи тональность сообщения как 'Позитивная', 'Негативная' или 'Нейтральная'. Ответь только одним словом."},
                    {"role": "user", "content": text}
                ],
                max_tokens=10,
                temperature=0
            )
            
            sentiment = response.choices[0].message.content.strip()
            return sentiment if sentiment in ["Позитивная", "Негативная", "Нейтральная"] else "Нейтральная"
            
        except Exception as e:
            self.logger.error(f"Ошибка при анализе тональности: {str(e)}")
            return "Не определено"

    def process_messages(self, messages: List[Dict]) -> pd.DataFrame:
        """Обрабатывает список сообщений и возвращает DataFrame"""
        processed_data = []
        
        for message in messages:
            text = message.get("text", "")
            cleaned_text = self.clean_text(text)
            
            processed_message = {
                "ID сообщения": message.get("message_id"),
                "Автор": message.get("author"),
                "Текст сообщения": cleaned_text,
                "Оригинальный текст": text,
                "Время": message.get("timestamp"),
                "Тип сообщения": message.get("type"),
                "Категория": self.categorize_message(cleaned_text),
                "Ключевые слова": ", ".join(self.extract_keywords(cleaned_text)),
                "Тональность": self.analyze_sentiment(cleaned_text),
                "Длина сообщения": len(cleaned_text),
                "Дата": datetime.fromisoformat(message.get("timestamp", datetime.now().isoformat())).strftime("%Y-%m-%d"),
                "Время суток": datetime.fromisoformat(message.get("timestamp", datetime.now().isoformat())).strftime("%H:%M")
            }
            
            processed_data.append(processed_message)
        
        df = pd.DataFrame(processed_data)
        
        # Добавляем дополнительные аналитические столбцы
        if not df.empty:
            df["День недели"] = pd.to_datetime(df["Дата"]).dt.day_name()
            df["Час"] = pd.to_datetime(df["Время суток"]).dt.hour
            
        return df

    def generate_summary(self, df: pd.DataFrame) -> Dict:
        """Генерирует сводную статистику по сообщениям"""
        if df.empty:
            return {}
            
        summary = {
            "Всего сообщений": len(df),
            "Уникальных авторов": df["Автор"].nunique(),
            "Средняя длина сообщения": df["Длина сообщения"].mean(),
            "Самые активные авторы": df["Автор"].value_counts().head(5).to_dict(),
            "Популярные категории": df["Категория"].value_counts().to_dict(),
            "Распределение тональности": df["Тональность"].value_counts().to_dict(),
            "Сообщения по часам": df.groupby("Час").size().to_dict(),
            "Сообщения по дням недели": df.groupby("День недели").size().to_dict()
        }
        
        return summary
