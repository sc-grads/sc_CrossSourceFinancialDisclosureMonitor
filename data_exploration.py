import yfinance as yf
import requests
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# Test 1: Yahoo Finance
def test_yahoo(ticker):
    stock = yf.Ticker(ticker)
    earnings = stock.earnings
    info = stock.info
    print(f"Yahoo - {ticker}")
    print(f"  Latest earnings: {earnings}")
    print(f"  EPS (TTM): {info.get('trailingEps')}")
    print(f"  Retrieved at: {datetime.now()}")
    print()

# Test 2: Financial Modeling Prep
def test_fmp(ticker, api_key):
    url = f"https://financialmodelingprep.com/stable/search-symbol?query={ticker}&apikey={api_key}"
    response = requests.get(url)
    data = response.json()
    print(f"FMP - {ticker}")
    
    # Check if data is a list and has elements
    if isinstance(data, list) and len(data) > 0:
        print(f"  Latest quarter: {data[0]}")
        print(f"  EPS: {data[0].get('eps', 'N/A')}")
    elif isinstance(data, dict) and 'Error Message' in data:
        print(f"  Error: {data['Error Message']}")
    else:
        print(f"  No data available or unexpected response: {data}")
    print()

# Test 3: Alpha Vantage
def test_alpha_vantage(ticker, api_key):
    url = f"https://www.alphavantage.co/query?function=EARNINGS&symbol={ticker}&apikey={api_key}"
    response = requests.get(url)
    data = response.json()
    print(f"Alpha Vantage - {ticker}")
    print(f"  Data: {data}")
    print()

if __name__ == "__main__":
    # Test with a company that recently reported (e.g., AAPL, MSFT, GOOGL)
    test_ticker = "AAPL"
    
    test_yahoo(test_ticker)
    test_fmp(test_ticker, os.getenv("FMP_API_KEY"))
    test_alpha_vantage(test_ticker, os.getenv("ALPHA_VANTAGE_API_KEY"))