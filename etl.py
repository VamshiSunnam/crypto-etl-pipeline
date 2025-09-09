import os
from dotenv import load_dotenv
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import numpy as np
from schema import Base, CryptoData
from sqlalchemy.orm import sessionmaker
from typing import List, Dict, Any, Optional
import fire
from prophet import Prophet
from sklearn.ensemble import IsolationForest
import requests as pyrequests
import json
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("etl.log"), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def notify_slack(message):
    if SLACK_WEBHOOK_URL:
        try:
            pyrequests.post(SLACK_WEBHOOK_URL, json={"text": message})
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")


# Decorator for error notification
from functools import wraps


def slack_notify_on_error(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            msg = f"ETL Pipeline Error in {func.__name__}: {e}"
            logger.error(msg)
            notify_slack(msg)
            raise

    return wrapper


# --- Candlestick Chart Visualization ---
def plot_candlestick(
    df: pd.DataFrame, coin_name: str, save_path: Optional[str] = None
) -> None:
    """
    Plots a candlestick chart for a specific cryptocurrency using available data.
    """
    # Filter for the coin
    coin_df = df[df["name"].str.lower() == coin_name.lower()]
    if coin_df.empty:
        logging.warning(f"No data found for coin: {coin_name}")
        return
    # For demo, use current, high, low as open/close/high/low (since only one day)
    # In real use, would need historical data
    ohlc = [
        [
            0,
            coin_df.iloc[0]["price_usd"],  # open
            coin_df.iloc[0]["high_24h"],  # high
            coin_df.iloc[0]["low_24h"],  # low
            coin_df.iloc[0]["price_usd"],  # close
        ]
    ]
    fig, ax = plt.subplots(figsize=(6, 4))
    # Draw candlestick manually
    for i, (date, open_, high, low, close) in enumerate(ohlc):
        color = "g" if close >= open_ else "r"
        ax.plot([i, i], [low, high], color="black")
        ax.add_patch(
            plt.Rectangle(
                (i - 0.2, min(open_, close)), 0.4, abs(close - open_), color=color
            )
        )
    ax.set_xticks([0])
    ax.set_xticklabels([coin_name])
    ax.set_ylabel("Price (USD)")
    ax.set_title(f"Candlestick Chart for {coin_name}")
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path)
        logging.info(f"Candlestick chart saved to {save_path}")
    else:
        plt.show()
    plt.close()


def plot_correlation(df: pd.DataFrame, save_path: Optional[str] = None) -> None:
    """
    Scatter plot of market cap vs 24h volume for all coins.
    """
    plt.figure(figsize=(8, 6))
    plt.scatter(df["market_cap"], df["volume_24h"], alpha=0.7)
    plt.xlabel("Market Cap (USD)")
    plt.ylabel("24h Volume (USD)")
    plt.title("Market Cap vs 24h Volume")
    plt.xscale("log")
    plt.yscale("log")
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path)
        logging.info(f"Correlation scatter plot saved to {save_path}")
    else:
        plt.show()
    plt.close()


@retry(stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=4, max=10),
       retry=retry_if_exception_type(requests.exceptions.RequestException))
def fetch_top_50_cryptos() -> List[Dict[str, Any]]:
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 50,
        "page": 1,
        "sparkline": False,
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        logger.info("Successfully fetched top 50 cryptocurrencies from CoinGecko API.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Network or API error fetching data: {e}")
        raise  # Re-raise to allow tenacity to catch and retry
    except Exception as e:
        logger.error(f"An unexpected error occurred during data fetching: {e}")
        raise


def transform_crypto_data(raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
    # Convert to DataFrame
    df = pd.DataFrame(raw_data)
    # Select relevant columns and rename for clarity
    columns = {
        "id": "id",
        "symbol": "symbol",
        "name": "name",
        "current_price": "price_usd",
        "market_cap": "market_cap",
        "total_volume": "volume_24h",
        "high_24h": "high_24h",
        "low_24h": "low_24h",
        "price_change_percentage_24h": "price_change_24h_pct",
        "circulating_supply": "circulating_supply",
        "max_supply": "max_supply",
        "last_updated": "last_updated",
    }
    df = df[list(columns.keys())].rename(columns=columns)
    # Clean: drop rows with missing essential data
    df = df.dropna(subset=["id", "symbol", "name", "price_usd"])
    # Reset index
    df = df.reset_index(drop=True)

    # --- Time-Series Features ---
    # For demo: group by 'id' and sort by 'last_updated' if multiple days are present
    # If only one day, these will be NaN or same as price_usd
    df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")
    df = df.sort_values(["id", "last_updated"])
    df["rolling_avg_7d"] = df.groupby("id")["price_usd"].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    )
    df["daily_price_change_pct"] = df.groupby("id")["price_usd"].pct_change() * 100
    logger.info(
        "Data transformed and cleaned using pandas, with time-series features added."
    )
    return df


# --- Data Validation Function ---
def validate_data(df: pd.DataFrame) -> None:
    """
    Validates the DataFrame for missing values and incorrect data types.
    Raises warnings or errors if issues are found.
    """
    issues = []
    # Check for missing values
    missing = df.isnull().sum()
    missing_cols = missing[missing > 0]
    if not missing_cols.empty:
        for col in missing_cols.index:
            if col == "max_supply":
                logger.warning(f"Missing values found in non-critical column: {col}")
            else:
                issues.append(f"Missing values found in critical column: {col}")
    # Check for expected dtypes
    expected_types = {
        "id": object,
        "symbol": object,
        "name": object,
        "price_usd": np.number,
        "market_cap": np.number,
        "volume_24h": np.number,
        "high_24h": np.number,
        "low_24h": np.number,
        "price_change_24h_pct": np.number,
        "circulating_supply": np.number,
        "max_supply": np.number,
        "last_updated": object,
    }
    for col, typ in expected_types.items():
        if col in df.columns:
            if typ is object:
                if not pd.api.types.is_object_dtype(df[col]):
                    issues.append(f"Column '{col}' has incorrect type: {df[col].dtype}")
            else:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    issues.append(
                        f"Column '{col}' should be numeric but is {df[col].dtype}"
                    )
    if issues:
        logger.warning("Data validation issues found:")
        for issue in issues:
            logger.warning(f"- {issue}")
        raise ValueError("Data validation failed. See issues above.")
    else:
        logger.info("Data validation passed. No issues found.")


def load_to_postgres(
    df: pd.DataFrame, db_url: str, table_name: str = "crypto_top_50"
) -> None:
    """
    Persists the DataFrame to a PostgreSQL table using SQLAlchemy ORM.
    """

    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        # Clear table first for replace behavior
        session.query(CryptoData).delete()
        session.commit()
        # Insert each row as ORM object
        records = []
        for _, row in df.iterrows():
            record = CryptoData(
                id=str(row["id"]),
                symbol=str(row["symbol"]),
                name=str(row["name"]),
                price_usd=float(row["price_usd"]),
                market_cap=float(row["market_cap"])
                if not pd.isnull(row["market_cap"])
                else None,
                volume_24h=float(row["volume_24h"])
                if not pd.isnull(row["volume_24h"])
                else None,
                high_24h=float(row["high_24h"])
                if not pd.isnull(row["high_24h"])
                else None,
                low_24h=float(row["low_24h"])
                if not pd.isnull(row["low_24h"])
                else None,
                price_change_24h_pct=float(row["price_change_24h_pct"])
                if not pd.isnull(row["price_change_24h_pct"])
                else None,
                circulating_supply=float(row["circulating_supply"])
                if not pd.isnull(row["circulating_supply"])
                else None,
                max_supply=float(row["max_supply"])
                if not pd.isnull(row["max_supply"])
                else None,
                last_updated=str(row["last_updated"])
                if not pd.isnull(row["last_updated"])
                else None,
            )
            records.append(record)
        session.bulk_save_objects(records)
        session.commit()
        logger.info(
            f"Data loaded to table '{table_name}' in PostgreSQL using ORM model."
        )
    except Exception as e:
        session.rollback()
        logger.error(f"Error loading data to PostgreSQL: {e}")
        raise
    finally:
        session.close()


# --- Visualization Functions ---
def plot_market_share(df: pd.DataFrame, save_path: str = "market_share.png") -> None:
    """
    Pie chart of market cap share for top 10 coins.
    """
    top10 = df.nlargest(10, "market_cap")
    plt.figure(figsize=(10, 7))
    plt.pie(
        top10["market_cap"], labels=top10["name"], autopct="%1.1f%%", startangle=140
    )
    plt.title("Top 10 Cryptocurrencies by Market Cap")
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()
    logger.info(f"Market share chart saved to {save_path}")


def plot_price_bar(df: pd.DataFrame, save_path: str = "price_bar.png") -> None:
    """
    Bar chart of current price for top 10 coins.
    """
    top10 = df.nlargest(10, "market_cap")
    plt.figure(figsize=(12, 6))
    plt.bar(top10["name"], top10["price_usd"], color="skyblue")
    plt.ylabel("Price (USD)")
    plt.title("Current Price of Top 10 Cryptocurrencies")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()
    logger.info(f"Price bar chart saved to {save_path}")


def detect_anomalies(df, coin_name, contamination=0.05):
    coin_df = df[df['name'].str.lower() == coin_name.lower()].copy()
    if coin_df.empty:
        logger.warning(f"No data found for coin: {coin_name}")
        return None
    coin_df = coin_df.sort_values('last_updated')
    X = coin_df[['price_usd']].values
    model = IsolationForest(contamination=contamination, random_state=42)
    coin_df['anomaly'] = model.fit_predict(X)
    return coin_df[['last_updated', 'price_usd', 'anomaly']]


def forecast_price(df, coin_name, periods=7):
    coin_df = df[df['name'].str.lower() == coin_name.lower()].copy()
    if coin_df.empty:
        logger.warning(f"No data found for coin: {coin_name}")
        return None
    coin_df = coin_df.sort_values('last_updated')
    prophet_df = coin_df[['last_updated', 'price_usd']].rename(columns={'last_updated': 'ds', 'price_usd': 'y'})
    m = Prophet()
    m.fit(prophet_df)
    future = m.make_future_dataframe(periods=periods)
    forecast = m.predict(future)
    return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]


LINEAGE_LOG = "lineage.log"


def log_lineage(step, status, extra=None):
    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "step": step,
        "status": status,
    }
    if extra:
        entry.update(extra)
    with open(LINEAGE_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


# Wrap CLI methods for lineage logging
def lineage_logger(step_name):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                log_lineage(step_name, "success")
                return result
            except Exception as e:
                log_lineage(step_name, "error", {"error": str(e)})
                raise

        return wrapper

    return decorator


class CryptoETLPipeline:
    @slack_notify_on_error
    @lineage_logger("extract")
    def extract(self, save_path: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetches live data for the top 50 cryptocurrencies from CoinGecko API. Optionally saves as JSON."""
        data = fetch_top_50_cryptos()
        if save_path:
            import json

            with open(save_path, "w") as f:
                json.dump(data, f, indent=2)
            logger.info(f"Raw data saved to {save_path}")
        return data

    @slack_notify_on_error
    @lineage_logger("transform")
    def transform(
        self, input_path: Optional[str] = None, output_path: Optional[str] = None
    ) -> pd.DataFrame:
        """Cleans and prepares data from JSON file or live, saves as CSV if output_path is given."""
        import json

        if input_path:
            with open(input_path) as f:
                raw_data = json.load(f)
        else:
            raw_data = fetch_top_50_cryptos()
        df = transform_crypto_data(raw_data)
        if output_path:
            df.to_csv(output_path, index=False)
            logger.info(f"Transformed data saved to {output_path}")
        return df

    @slack_notify_on_error
    @lineage_logger("load")
    def load(
        self,
        csv_path: str,
        db_url: Optional[str] = None,
        table_name: str = "crypto_top_50",
    ) -> None:
        """Loads cleaned data from CSV into PostgreSQL."""
        if not db_url:
            user = os.getenv("POSTGRES_USER")
            password = os.getenv("POSTGRES_PASSWORD")
            host = os.getenv("POSTGRES_HOST")
            port = os.getenv("POSTGRES_PORT", "5432")
            db = os.getenv("POSTGRES_DB")
            if not all([user, password, host, port, db]):
                logger.error(
                    "Missing database credentials in .env file. Please check your .env."
                )
                return
            db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        df = pd.read_csv(csv_path)
        load_to_postgres(df, db_url, table_name)

    @slack_notify_on_error
    @lineage_logger("validate")
    def validate(self, csv_path: str) -> None:
        """Validates the cleaned CSV data for missing values and type issues."""
        df = pd.read_csv(csv_path)
        validate_data(df)

    @slack_notify_on_error
    @lineage_logger("anomalies")
    def anomalies(self, csv_path, coin_name, contamination=0.05):
        """Detects anomalies in price for a specific coin using Isolation Forest."""
        df = pd.read_csv(csv_path, parse_dates=['last_updated'])
        result = detect_anomalies(df, coin_name, contamination)
        if result is not None:
            result.to_csv(f"{coin_name}_anomalies.csv", index=False)
            logger.info(f"Anomaly detection results saved to {coin_name}_anomalies.csv")
        else:
            logger.warning("No results to save.")

    @slack_notify_on_error
    @lineage_logger("forecast")
    def forecast(self, csv_path, coin_name, periods=7):
        """Forecasts price for a specific coin using Prophet."""
        df = pd.read_csv(csv_path, parse_dates=['last_updated'])
        forecast = forecast_price(df, coin_name, periods)
        if forecast is not None:
            forecast.to_csv(f"{coin_name}_forecast.csv", index=False)
            logger.info(f"Forecast results saved to {coin_name}_forecast.csv")
        else:
            logger.warning("No forecast to save.")


if __name__ == "__main__":
    fire.Fire(CryptoETLPipeline)
