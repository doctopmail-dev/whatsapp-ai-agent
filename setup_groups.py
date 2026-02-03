#!/usr/bin/env python3
"""
Утилита для настройки и получения информации о WhatsApp группах
"""

import os
from dotenv import load_dotenv
from whatsapp_client import WhatsAppClient

load_dotenv()

def setup_groups():
    """Помогает настроить группы для мониторинга"""
    
    # Инициализация клиента
    client = WhatsAppClient(
        api_token=os.getenv('WHATSAPP_API_TOKEN'),
        phone_number_id=os.getenv('WHATSAPP_PHONE_NUMBER_ID')
    )
    
    print("🔍 Поиск WhatsApp групп...")
    
    try:
        groups = client.get_groups_list()
        
        if not groups:
            print("❌ Группы не найдены. Убедитесь, что:")
            print("   - WhatsApp API настроен правильно")
            print("   - Ваш номер добавлен в группы")
            print("   - API токен действителен")
            return
        
        print(f"\n📋 Найдено {len(groups)} групп:")
        print("-" * 80)
        
        for i, group in enumerate(groups, 1):
            name = group.get('name', 'Без названия')
            group_id = group.get('id', 'N/A')
            participants_count = len(group.get('participants', []))
            
            print(f"{i:2d}. {name}")
            print(f"     ID: {group_id}")
            print(f"     Участников: {participants_count}")
            print()
        
        # Создаем конфигурацию
        print("🔧 Генерация конфигурации для .env файла:")
        print("-" * 50)
        
        group_ids = [group['id'] for group in groups]
        group_ids_str = ",".join(group_ids)
        
        print(f"WHATSAPP_GROUP_IDS={group_ids_str}")
        print(f"MESSAGES_PER_GROUP=50")
        
        # Сохраняем в файл
        with open('group_ids.txt', 'w') as f:
            f.write(f"WHATSAPP_GROUP_IDS={group_ids_str}\n")
            f.write(f"MESSAGES_PER_GROUP=50\n")
        
        print(f"\n💾 Конфигурация сохранена в файл group_ids.txt")
        print("Скопируйте эти строки в ваш .env файл")
        
        # Тестовый сбор сообщений
        print("\n🧪 Тестовый сбор сообщений из первой группы...")
        if groups:
            first_group_id = groups[0]['id']
            messages = client.get_group_messages(first_group_id, limit=5)
            
            if messages:
                print(f"✅ Успешно получено {len(messages)} сообщений")
                print("\nПример сообщения:")
                sample = client.process_message(messages[0])
                print(f"   Автор: {sample['author']}")
                print(f"   Текст: {sample['text'][:100]}...")
                print(f"   Время: {sample['timestamp']}")
            else:
                print("❌ Не удалось получить сообщения")
        
    except Exception as e:
        print(f"❌ Ошибка: {str(e)}")
        print("\nПроверьте настройки WhatsApp API в .env файле")

def test_specific_group(group_id):
    """Тестирует работу с конкретной группой"""
    
    client = WhatsAppClient(
        api_token=os.getenv('WHATSAPP_API_TOKEN'),
        phone_number_id=os.getenv('WHATSAPP_PHONE_NUMBER_ID')
    )
    
    print(f"🧪 Тестирование группы ID: {group_id}")
    
    try:
        messages = client.get_group_messages(group_id, limit=10)
        
        if messages:
            print(f"✅ Получено {len(messages)} сообщений")
            
            # Анализ сообщений
            authors = {}
            for msg in messages:
                processed = client.process_message(msg)
                author = processed['author']
                authors[author] = authors.get(author, 0) + 1
            
            print("\n📊 Активность авторов:")
            for author, count in sorted(authors.items(), key=lambda x: x[1], reverse=True):
                print(f"   {author}: {count} сообщений")
                
        else:
            print("❌ Сообщения не найдены")
            
    except Exception as e:
        print(f"❌ Ошибка: {str(e)}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Тестируем конкретную группу
        group_id = sys.argv[1]
        test_specific_group(group_id)
    else:
        # Настраиваем все группы
        setup_groups()
