import pytest
import requests
from etl import fetch_top_50_cryptos


def test_fetch_top_50_cryptos_success(requests_mock):
    # Mock the CoinGecko API response
    mock_response = [
        {"id": "bitcoin", "name": "Bitcoin", "current_price": 50000},
        {"id": "ethereum", "name": "Ethereum", "current_price": 3000},
    ]
    requests_mock.get(
        "https://api.coingecko.com/api/v3/coins/markets",
        json=mock_response,
        status_code=200,
    )

    # Call the function
    data = fetch_top_50_cryptos()

    # Assertions
    assert isinstance(data, list)
    assert len(data) == 2
    assert data[0]["id"] == "bitcoin"
    assert data[1]["name"] == "Ethereum"


def test_fetch_top_50_cryptos_api_error(requests_mock):
    # Mock an API error response
    requests_mock.get("https://api.coingecko.com/api/v3/coins/markets", status_code=500)

    # Expect an exception to be raised
    with pytest.raises(requests.exceptions.HTTPError):
        fetch_top_50_cryptos()
