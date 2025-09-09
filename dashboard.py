import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import numpy as np

# API Base URL (assuming API is running on localhost:8000 or via Docker Compose service name 'api')
API_BASE_URL = "http://api:8000/api/v1/cryptos" # Use 'api' as hostname if running in Docker Compose

st.set_page_config(layout="wide")
st.title("Cryptocurrency Data Explorer")

@st.cache_data(ttl=600) # Cache data for 10 minutes
def fetch_data(endpoint: str = ""):
    try:
        response = requests.get(f"{API_BASE_URL}{endpoint}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data from API: {e}")
        return None

# Fetch all crypto data
all_cryptos_data = fetch_data()

if all_cryptos_data:
    df_all_cryptos = pd.DataFrame(all_cryptos_data)

    st.header("All Cryptocurrencies")
    st.dataframe(df_all_cryptos)

    # --- Market Share Pie Chart ---
    st.header("Top 10 Cryptocurrencies by Market Cap")
    top10_market_cap_data = fetch_data("/top10_market_cap")
    if top10_market_cap_data:
        df_top10_market_cap = pd.DataFrame(top10_market_cap_data)
        fig_pie = px.pie(df_top10_market_cap, values='market_cap', names='name',
                         title='Market Cap Distribution of Top 10 Cryptos')
        st.plotly_chart(fig_pie, use_container_width=True)

    # --- Price Bar Chart (Top 10 by Market Cap) ---
    st.header("Current Price of Top 10 Cryptocurrencies")
    if top10_market_cap_data:
        df_top10_market_cap = pd.DataFrame(top10_market_cap_data)
        fig_bar = px.bar(df_top10_market_cap, x='name', y='price_usd',
                         title='Current Price of Top 10 Cryptos', labels={'price_usd': 'Price (USD)'})
        st.plotly_chart(fig_bar, use_container_width=True)

    # --- Candlestick Chart (Interactive Selection) ---
    st.header("Candlestick Chart")
    coin_names = df_all_cryptos['name'].tolist()
    selected_coin = st.selectbox("Select a cryptocurrency for Candlestick Chart", coin_names)

    if selected_coin:
        # Fetch data for the selected coin (simplified for demo, ideally API would provide historical OHLCV)
        selected_coin_data = df_all_cryptos[df_all_cryptos['name'] == selected_coin].iloc[0]
        
        # For demo, use current, high, low as open/close/high/low (since only one day)
        # In real use, would need historical data from API
        ohlc_data = pd.DataFrame({
            'Date': [pd.to_datetime(selected_coin_data['last_updated'])],
            'Open': [selected_coin_data['price_usd']],
            'High': [selected_coin_data['high_24h']],
            'Low': [selected_coin_data['low_24h']],
            'Close': [selected_coin_data['price_usd']]
        })

        fig_candlestick = px.candlestick(ohlc_data, x='Date', open='Open', high='High', low='Low', close='Close',
                                         title=f'Candlestick Chart for {selected_coin}')
        st.plotly_chart(fig_candlestick, use_container_width=True)

    # --- Market Cap vs 24h Volume Correlation Plot ---
    st.header("Market Cap vs 24h Volume Correlation")
    market_cap_volume_data = fetch_data("/market_cap_vs_volume")
    if market_cap_volume_data:
        df_market_cap_volume = pd.DataFrame(market_cap_volume_data)
        fig_scatter = px.scatter(df_market_cap_volume, x='market_cap', y='volume_24h',
                                 log_x=True, log_y=True, hover_name='name',
                                 title='Market Cap vs 24h Volume')
        st.plotly_chart(fig_scatter, use_container_width=True)
else:
    st.warning("Could not fetch data from the API. Please ensure the API is running and accessible.")
