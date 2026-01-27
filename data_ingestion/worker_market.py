# worker_market.py
from core.market_router import MarketRouter # <--- Novo componente
from core.db import DatabaseManager
from core.env import load_config
from core.logger import get_logger

logger = get_logger("WorkerMarket")

def fetch_and_save():
    config = load_config()
    db = DatabaseManager(use_service_role=True)
    router = MarketRouter(config) # Injeção de Dependência via Config
    
    tickers = config.get('tickers', [])
    
    logger.info("🚀 Iniciando Ingestão Híbrida (Padrão Institucional)...")
    
    # O Router cuida da complexidade de APIs
    df_unified = router.fetch_batch(tickers)
    
    if df_unified.empty:
        logger.error("❌ Nenhum dado coletado de nenhuma fonte.")
        return

    # Transformação para formato do Banco
    records = []
    for index, row in df_unified.iterrows():
        records.append({
            "ticker": row['ticker'],
            "date": index.strftime('%Y-%m-%d'),
            "open": float(row['Open']),
            "high": float(row['High']),
            "low": float(row['Low']),
            "close": float(row['Close']),
            "volume": float(row['Volume']),
            "source": row.get('source', 'UNKNOWN') # <--- CAMPO NOVO DE AUDITORIA
        })

    # Persistência
    if records:
        logger.info(f"💾 Salvando {len(records)} registros com rastreabilidade de fonte.")
        db.client.table("market_prices").upsert(records, on_conflict="ticker, date").execute()

if __name__ == "__main__":
    fetch_and_save()