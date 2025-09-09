from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from schema import Base, CryptoData
from typing import List, Dict, Any, Optional
import pandas as pd
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Database connection setup
def get_db_url():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB")
    if not all([user, password, host, db]):
        logger.error("Missing database credentials in .env file for API. Please check your .env.")
        raise ValueError("Database credentials not fully provided.")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

DATABASE_URL = get_db_url()
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/api/v1/cryptos", response_model=List[Dict[str, Any]])
def get_all_cryptos():
    db = SessionLocal()
    try:
        cryptos = db.query(CryptoData).all()
        return [
            {
                "id": c.id,
                "symbol": c.symbol,
                "name": c.name,
                "price_usd": c.price_usd,
                "market_cap": c.market_cap,
                "volume_24h": c.volume_24h,
                "high_24h": c.high_24h,
                "low_24h": c.low_24h,
                "price_change_24h_pct": c.price_change_24h_pct,
                "circulating_supply": c.circulating_supply,
                "max_supply": c.max_supply,
                "last_updated": c.last_updated,
            }
            for c in cryptos
        ]
    except Exception as e:
        logger.error(f"Error fetching all cryptos: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        db.close()

@app.get("/api/v1/cryptos/{coin_id}", response_model=Dict[str, Any])
def get_crypto_by_id(coin_id: str):
    db = SessionLocal()
    try:
        crypto = db.query(CryptoData).filter(CryptoData.id == coin_id).first()
        if crypto is None:
            raise HTTPException(status_code=404, detail="Crypto not found")
        return {
            "id": crypto.id,
            "symbol": crypto.symbol,
            "name": crypto.name,
            "price_usd": crypto.price_usd,
            "market_cap": crypto.market_cap,
            "volume_24h": crypto.volume_24h,
            "high_24h": crypto.high_24h,
            "low_24h": crypto.low_24h,
            "price_change_24h_pct": crypto.price_change_24h_pct,
            "circulating_supply": crypto.circulating_supply,
            "max_supply": crypto.max_supply,
            "last_updated": crypto.last_updated,
        }
    except HTTPException:
        raise # Re-raise HTTPException
    except Exception as e:
        logger.error(f"Error fetching crypto by ID {coin_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        db.close()

@app.get("/api/v1/cryptos/top10_market_cap", response_model=List[Dict[str, Any]])
def get_top10_market_cap():
    db = SessionLocal()
    try:
        # Order by market_cap descending and limit to 10
        cryptos = db.query(CryptoData).order_by(CryptoData.market_cap.desc()).limit(10).all()
        return [
            {
                "id": c.id,
                "symbol": c.symbol,
                "name": c.name,
                "price_usd": c.price_usd,
                "market_cap": c.market_cap,
            }
            for c in cryptos
        ]
    except Exception as e:
        logger.error(f"Error fetching top 10 by market cap: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        db.close()

@app.get("/api/v1/cryptos/market_cap_vs_volume", response_model=List[Dict[str, Any]])
def get_market_cap_vs_volume():
    db = SessionLocal()
    try:
        cryptos = db.query(CryptoData).all()
        return [
            {
                "name": c.name,
                "market_cap": c.market_cap,
                "volume_24h": c.volume_24h,
            }
            for c in cryptos
        ]
    except Exception as e:
        logger.error(f"Error fetching market cap vs volume data: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        db.close()
