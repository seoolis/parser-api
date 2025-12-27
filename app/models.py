# models.py

# Определение моделей данных: ORM-модель для базы и Pydantic-схемы для API

from sqlmodel import SQLModel, Field
from pydantic import BaseModel

# Модель данных: валюта с курсом
class CurrencyRate(SQLModel, table=True):
    """
    ORM-модель для хранения курсов валют
    Поля:
    - id: автоинкрементный первичный ключ
    - code: уникальный код валюты (например, USD)
    - name: полное название
    - rate: текущий курс к рублю
    - updated_at: время последнего обновления (в формате HH:MM:SS)
    """

    # Название таблицы в SQLite
    __tablename__ = "currency_rates"

    # Уникальный ID записи (автоматически генерируется)
    id: int | None = Field(default=None, primary_key=True)
    # Код валюты должен быть уникальным и индексироваться для быстрого поиска
    code: str = Field(index=True, unique=True)
    # Полное название валюты
    name: str
    # Текущий курс валюты
    rate: float
    # Время последнего обновления в формате "HH:MM:SS"
    updated_at: str


# Схема для ответа API, pydantic-схема
class CurrencyRateResponse(BaseModel):
    id: int
    code: str
    name: str
    rate: float
    updated_at: str

    model_config = {"from_attributes": True}  # Важно для SQLModel


# Pydantic-схема для создания новой валюты через POST
class CurrencyRateCreate(BaseModel):
    code: str
    name: str
    rate: float