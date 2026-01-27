# ARQUIVO: core/market_data.py
import pandas as pd
import time
from core.db import DatabaseManager
from core.logger import get_logger

# Configura log
logger = get_logger(__name__)

class MarketDataError(Exception):
    """Exceção customizada para falhas críticas na camada de dados de mercado."""
    pass

class MarketLoader:
    @staticmethod
    def get_market_data(tickers: list, period="6mo"):
        """
        Busca dados BLINDADOS do Supabase.
        Implementa lógica de Retry para erros de 'Device busy' e validação rigorosa.
        """
        if not tickers:
            logger.error("Tentativa de buscar dados com lista de tickers vazia.")
            raise MarketDataError("A lista de tickers fornecida está vazia.")

        logger.info(f"Buscando dados no DB para {len(tickers)} ativos (Período: {period})")
        
        # Instancia o DatabaseManager
        db = DatabaseManager(use_service_role=False)
        
        # --- CONFIGURAÇÃO DO RETRY (BLINDAGEM) ---
        max_retries = 5
        backoff_factor = 2 # Segundos iniciais de espera

        for attempt in range(max_retries):
            try:
                # Tenta executar a query
                # Nota: O limite de 2000 linhas é um buffer de segurança
                response = db.client.table("market_prices")\
                    .select("ticker, close, date")\
                    .in_("ticker", tickers)\
                    .order("date", desc=True)\
                    .limit(2000)\
                    .execute()
                
                data = response.data
                
                # VALIDAÇÃO 1: Banco vazio
                if not data:
                    msg = "Cache do Supabase está vazio. O Worker de mercado rodou com sucesso?"
                    logger.warning(msg)
                    # Se não tem dados, não adianta tentar de novo, é erro de lógica/worker
                    raise MarketDataError(msg)

                # Processamento (Pivot)
                df_db = pd.DataFrame(data)
                df_pivot = df_db.pivot(index='date', columns='ticker', values='close')
                
                # Validação de tickers ausentes (Transformamos em INFO em vez de WARNING se for esperado)
                missing_tickers = set(tickers) - set(df_pivot.columns)
                if missing_tickers:
                    logger.info(f"ℹ️ Tickers ausentes no cache (ignorado no cálculo): {missing_tickers}")
                
                # Preenchimento de lacunas (Forward Fill)
                df_pivot = df_pivot.ffill()
                
                # Se um ticker essencial (como USDBRL=X) estiver faltando, aí sim lançamos erro
                if "USDBRL=X" not in df_pivot.columns:
                    raise MarketDataError("Ativo crítico (Dólar) ausente no banco de dados.")

                # VALIDAÇÃO 2: DataFrame vazio pós-processamento
                if df_pivot.empty:
                    raise MarketDataError("DataFrame resultante do pivot está vazio após processamento.")
                
                logger.info(f"✅ Dados carregados com sucesso. Shape: {df_pivot.shape}")
                
                # LOG DE AUDITORIA: Indica que os dados estão vindo de uma fonte sandbox
                logger.warning("⚠️ DATA SOURCE: Using SANDBOX (Yahoo Finance) for PoC purposes. Latency: ~15min.")
                
                return df_pivot

            except MarketDataError as e:
                # Se o erro for de lógica (ex: dados vazios), não faz retry, falha logo.
                raise e

            except Exception as e:
                error_msg = str(e)
                # Verifica se é o erro de recurso ocupado ou conexão
                if "Device or resource busy" in error_msg or "ConnectError" in error_msg:
                    # Se ainda tiver tentativas, espera e tenta de novo
                    if attempt < max_retries - 1:
                        wait_time = backoff_factor * (attempt + 1)
                        logger.warning(f"⏳ OS Busy/Erro de Conexão (Tentativa {attempt+1}/{max_retries}). Esperando {wait_time}s... Erro: {error_msg}")
                        time.sleep(wait_time)
                        continue # Vai para a próxima iteração do loop
                
                # Se esgotou as tentativas ou é um erro desconhecido crítico
                logger.error(f"❌ Falha crítica após {attempt+1} tentativas: {error_msg}", exc_info=True)
                raise MarketDataError(f"Falha de infraestrutura no Supabase: {error_msg}")