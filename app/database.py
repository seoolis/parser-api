# database.py

# Настройка подключения к базе данных и управление сессиями

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from fastapi import Depends

# Путь к файлу базы данных SQLite (асинхронная версия с использованием aiosqlite)
DB_URL = "sqlite+aiosqlite:///./currency_database.db"

# Асинхронный движок SQLAlchemy, он управляет подключением к БД
engine = create_async_engine(DB_URL, echo=False) # echo=False означает, что не будет вывода SQL-запросов в консоль

# Фабрика асинхронных сессий
AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# Зависимость FastAPI: внедряет сессию БД в эндпоинты
async def get_db():
    # Новая сессия на время запроса
    async with AsyncSessionLocal() as session:
        # Передача сессии
        yield session
    # Сессия автоматически закрывается после завершения запроса