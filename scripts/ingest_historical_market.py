import pandas as pd
import yfinance as yf
from datetime import datetime
from core.db import DatabaseManager
from core.env import load_config
from core.logger import get_logger

logger = get_logger("HistoricalMarketIngestor")

def ingest_historical_prices():
    """
    Popula a tabela market_prices com dados reais da Safra 23/24.
    Nível Ouro: Garante que o backtest rode sobre preços históricos reais.
    """
    db = DatabaseManager(use_service_role=True)
    config = load_config()
    
    # Tickers essenciais para o motor de risco
    tickers = config.get('tickers', ["ZS=F", "USDBRL=X", "CL=F"])
    
    # Janela da Safra 23/24 (com margem de segurança para médias móveis)
    start_date = "2023-01-01"
    end_date = "2024-06-01"

    logger.info(f"📅 Buscando dados históricos para: {tickers}")

    try:
        # Download massivo via Yahoo Finance
        data = yf.download(tickers, start=start_date, end=end_date, group_by='ticker')
        
        records_to_upsert = []

        for ticker in tickers:
            df_ticker = data[ticker].dropna()
            logger.info(f"📈 Processando {ticker}: {len(df_ticker)} registros encontrados.")

            for date, row in df_ticker.iterrows():
                records_to_upsert.append({
                    "ticker": ticker,
                    "date": date.strftime('%Y-%m-%d'),
                    "open": float(row['Open']),
                    "high": float(row['High']),
                    "low": float(row['Low']),
                    "close": float(row['Close']),
                    "volume": float(row['Volume']) if 'Volume' in row else 0
                })

        if records_to_upsert:
            # Upsert institucional: evita duplicatas e garante integridade
            logger.info(f"🚀 Enviando {len(records_to_upsert)} registros para o Supabase...")
            db.client.table("market_prices").upsert(
                records_to_upsert, 
                on_conflict="ticker, date"
            ).execute()
            logger.info("✅ Dados históricos de mercado integrados com sucesso.")
        
    except Exception as e:
        logger.error(f"❌ Falha na ingestão histórica: {e}")

if __name__ == "__main__":
    ingest_historical_prices()