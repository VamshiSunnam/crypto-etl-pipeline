from sqlalchemy import Column, Float, String
from sqlalchemy.ext.declarative import declarative_base
from typing import Optional

Base = declarative_base()


class CryptoData(Base):
    __tablename__ = "crypto_top_50"

    id: str = Column(String, primary_key=True)
    symbol: str = Column(String)
    name: str = Column(String)
    price_usd: float = Column(Float)
    market_cap: Optional[float] = Column(Float, nullable=True)
    volume_24h: Optional[float] = Column(Float, nullable=True)
    high_24h: Optional[float] = Column(Float, nullable=True)
    low_24h: Optional[float] = Column(Float, nullable=True)
    price_change_24h_pct: Optional[float] = Column(Float, nullable=True)
    circulating_supply: Optional[float] = Column(Float, nullable=True)
    max_supply: Optional[float] = Column(Float, nullable=True)
    last_updated: Optional[str] = Column(String, nullable=True)
