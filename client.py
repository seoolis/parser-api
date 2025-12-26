import asyncio
import httpx


async def test_full_api():
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8000") as client:
        print("1. [HEALTH] Проверка систем...")
        health = (await client.get("/health")).json()
        print(f"Результат: {health}")

        print("\n2. [TASK] Запуск парсера...")
        await client.post("/tasks/run")
        await asyncio.sleep(2)  # Даем время БД обновиться

        print("\n3. [GET] Получение списка...")
        items = (await client.get("/items")).json()
        if not items:
            print("База пуста!")
            return

        target_id = items[0]['id']
        print(f"В базе {len(items)} записей. Берем ID: {target_id}")

        print(f"\n4. [GET ID] Получение валюты с ID {target_id}...")
        item = (await client.get(f"/items/{target_id}")).json()
        print(f"Найдено: {item['code']} - {item['rate']}")

        print(f"\n5. [PATCH] Ручное изменение курса для ID {target_id}...")
        patched = (await client.patch(f"/items/{target_id}", json={"new_rate": 99.99})).json()
        print(f"Новый курс: {patched['rate']} (обновлено в {patched['updated_at']})")

        print("\n--- Тест завершен успешно ---")


if __name__ == "__main__":
    asyncio.run(test_full_api())