# models.py

from sqlmodel import SQLModel, Field
from pydantic import BaseModel

# Модель данных: валюта с курсом
class CurrencyRate(SQLModel, table=True):
    """
    ORM-модель для хранения курсов валют в SQLite.
    Поля:
    - id: автоинкрементный первичный ключ
    - code: уникальный код валюты (например, USD)
    - name: полное название
    - rate: текущий курс к рублю
    - updated_at: время последнего обновления (в формате HH:MM:SS)
    """
    __tablename__ = "currency_rates"

    id: int | None = Field(default=None, primary_key=True)
    code: str = Field(index=True, unique=True)
    name: str
    rate: float
    updated_at: str


# Pydantic-схема для ответа API (поддерживает ORM-объекты)
class CurrencyRateResponse(BaseModel):
    """
    Схема для сериализации объекта CurrencyRate при ответе по REST API.
    """
    id: int
    code: str
    name: str
    rate: float
    updated_at: str

    model_config = {"from_attributes": True}  # Важно для SQLModel


# Pydantic-схема для создания новой валюты через POST
class CurrencyRateCreate(BaseModel):
    """
    Схема для валидации входящих данных при создании валюты.
    """
    code: str
    name: str
    rate: float