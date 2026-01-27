# ARQUIVO: core/brapi_client.py
import requests
import pandas as pd
import os
from datetime import datetime
from core.logger import get_logger

logger = get_logger("BrapiClient")

class BrapiClient:
    BASE_URL = "https://brapi.dev/api"

    def __init__(self):
        self.token = os.getenv("BRAPI_TOKEN")
        self.session = requests.Session()

    def get_historical_data(self, ticker: str, range_str="3mo", interval="1d") -> pd.DataFrame:
        """
        Busca dados históricos (OHLC) formatados igual ao yfinance.
        ticker: 'USDBRL', 'RAIL3', 'PETR4'
        """
        if not self.token:
            logger.error("Token Brapi não configurado.")
            return pd.DataFrame()

        try:
            # Endpoint de Quote com range histórico
            url = f"{self.BASE_URL}/quote/{ticker}"
            params = {
                'token': self.token,
                'range': range_str,
                'interval': interval,
                'fundamental': 'false'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if 'results' not in data or not data['results']:
                return pd.DataFrame()

            # Extrai a série histórica
            historical = data['results'][0].get('historicalDataPrice', [])
            if not historical:
                return pd.DataFrame()

            # Transforma em DataFrame Pandas
            df = pd.DataFrame(historical)
            
            # Padroniza colunas para o formato do seu sistema (igual yfinance)
            # Brapi retorna: date, open, high, low, close, volume
            df = df.rename(columns={
                'date': 'Date',
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            })
            
            # Converte data (timestamp unix ou string) para datetime
            # A Brapi geralmente manda timestamp. Ajuste conforme retorno.
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'], unit='s') 
            
            df.set_index('Date', inplace=True)
            return df

        except Exception as e:
            logger.error(f"Erro Brapi histórico para {ticker}: {e}")
            return pd.DataFrame()