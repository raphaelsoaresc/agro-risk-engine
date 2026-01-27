# core/market_router.py
from core.brapi_client import BrapiClient
import yfinance as yf
import pandas as pd
from core.logger import get_logger

logger = get_logger("MarketRouter")

class MarketRouter:
    def __init__(self, config):
        self.config = config
        self.brapi = BrapiClient()
        self.sources_map = config.get('market_sources', {}).get('overrides', {})
        self.translations = config.get('ticker_translation', {})

    def fetch_batch(self, tickers):
        """
        Orquestra a busca dividindo os tickers entre os provedores corretos.
        Retorna um DataFrame unificado e normalizado.
        """
        brapi_tickers = []
        yahoo_tickers = []
        
        # 1. Roteamento
        for t in tickers:
            source = self.sources_map.get(t, 'yahoo') # Default é Yahoo
            if source == 'brapi':
                brapi_tickers.append(t)
            else:
                yahoo_tickers.append(t)

        results = []

        # 2. Execução Brapi (Iterativo pois a API free pode não suportar batch complexo)
        for t in brapi_tickers:
            # Traduz o ticker do sistema para o ticker da API
            api_symbol = self.translations.get('brapi', {}).get(t, t)
            logger.info(f"🇧🇷 Roteando {t} -> Brapi ({api_symbol})")
            
            df = self.brapi.get_historical_data(api_symbol) # Método que criamos antes
            if not df.empty:
                df['ticker'] = t # Garante que o DF tenha o ticker do sistema
                df['source'] = 'BRAPI' # AUDITABILIDADE (Essencial para Institucional)
                results.append(df)
            else:
                logger.warning(f"⚠️ Falha Brapi para {t}. Tentando Fallback Yahoo.")
                yahoo_tickers.append(t) # Fallback automático

        # 3. Execução Yahoo (Batch)
        if yahoo_tickers:
            logger.info(f"🇺🇸 Roteando {len(yahoo_tickers)} ativos -> Yahoo Finance")
            try:
                # Otimização: Threads=True
                yf_data = yf.download(yahoo_tickers, period="5d", progress=False, threads=True, group_by='ticker')
                
                # Normalização chata do Yahoo MultiIndex
                for t in yahoo_tickers:
                    try:
                        df_t = yf_data[t].dropna().copy() if len(yahoo_tickers) > 1 else yf_data.dropna().copy()
                        if not df_t.empty:
                            df_t['ticker'] = t
                            df_t['source'] = 'YAHOO'
                            results.append(df_t)
                    except KeyError:
                        logger.error(f"Yahoo não retornou dados para {t}")
            except Exception as e:
                logger.error(f"Erro crítico Yahoo: {e}")

        # 4. Unificação
        if not results:
            return pd.DataFrame()
            
        return pd.concat(results)