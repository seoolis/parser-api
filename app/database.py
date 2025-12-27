# database.py

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from fastapi import Depends

# URL SQLite-базы (асинхронная версия)
DB_URL = "sqlite+aiosqlite:///./currency_database.db"

# Асинхронный движок SQLAlchemy
engine = create_async_engine(DB_URL, echo=False)

# Фабрика асинхронных сессий
AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# Зависимость FastAPI: внедряет сессию БД в эндпоинты
async def get_db():
    """
    Генератор асинхронной сессии БД.
    Используется как зависимость в маршрутах: `db: AsyncSession = Depends(get_db)`.
    """
    async with AsyncSessionLocal() as session:
        yield session