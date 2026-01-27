import pytz
import pandas as pd
import numpy as np
from datetime import datetime
import os

# Core Imports
from core.env import load_config
from core.market_data import MarketLoader
from core.engine import RiskEngine
from core.db import DatabaseManager
from core.climate_risk import ClimateIntelligence
from core.scout import NewsScout
from core.logger import get_logger
from core.advisor import RiskAdvisor # 1. Certifique-se de que o import existe

# Componentes Refatorados
from core.context import RiskContext
from core.persister import RiskPersister
from core.factory import RegionalEngineFactory

# INSTANCIAÇÃO GLOBAL DO LOGGER (Nível de Módulo)
logger = get_logger(__name__) 

class RiskPipeline:
    def __init__(self, mode: str):
        self.mode = mode
        self.config = load_config()
        
        # Infraestrutura
        self.db = DatabaseManager(use_service_role=True)
        self.engine = RiskEngine()
        self.climate_intel = ClimateIntelligence()
        self.scout = NewsScout(use_service_role=True)
        
        # 2. INICIALIZAÇÃO DO ADVISOR (O que estava faltando)
        self.advisor = RiskAdvisor() 
        
        # Componentes de Apoio
        self.context = RiskContext()
        self.persister = RiskPersister(self.db)
        
        self.br_tz = pytz.timezone('America/Sao_Paulo')
        self.now_br = datetime.now(self.br_tz)
        
        self.macro_corr = 0.0 
        # Puxa o mapa de tickers diretamente da config já carregada
        self.ticker_map = self.config.get("ticker_map", {})

    def run(self):
        logger.info(f"🚀 Iniciando Credit Risk Pipeline")
        
        # --- CORREÇÃO: Executar o Scout antes de processar contratos ---
        # Como o fetch_and_store é async, e o run é sync, precisamos rodar via asyncio.run
        # ou transformar o run em async. Para manter simples aqui:
        import asyncio
        try:
            logger.info("🕵️ Executando Scout de Notícias (IA)...")
            asyncio.run(self.scout.fetch_and_store())
        except Exception as e:
            logger.error(f"⚠️ Falha no Scout de Notícias (não bloqueante): {e}")
        # ---------------------------------------------------------------
        
        response = self.db.client.table("credit_portfolio").select("*").execute()
        self.contracts = response.data or []
        
        if not self.contracts:
            logger.warning("⚠️ Nenhum contrato encontrado.")
            return

        if not self._load_data(): return

        # Chama a versão inteligente do cálculo de correlação
        self.macro_corr = self._calculate_macro_correlation()

        self._process_contracts()  # Método renomeado

        # Notificação de Resumo (Morning Call) apenas para o Admin
        if self.mode == "morning":
            admin_email = os.getenv("EMAIL_TO")
            self.notifier.check_and_send(self.mode, self.context, self.df_market, recipient_email=admin_email)

        logger.info("✅ Pipeline finalizado com sucesso.")

    def _calculate_macro_correlation(self) -> float:
        """Calcula correlação entre a commodity principal da carteira e o Dólar."""
        try:
            if self.df_market is None or "USDBRL=X" not in self.df_market.columns:
                return 0.0

            # Pega a cultura do primeiro contrato como referência da carteira
            first_contract = self.contracts[0]
            culture = (first_contract.get("culture") or first_contract.get("commodity", "")).lower()
            primary_ticker = self.ticker_map.get(culture, "ZS=F")

            if primary_ticker not in self.df_market.columns:
                primary_ticker = "ZS=F"

            # Correlação de 30 dias (Retornos percentuais são mais precisos que preços brutos)
            df_recent = self.df_market.tail(30).pct_change()
            correlation = df_recent[primary_ticker].corr(df_recent["USDBRL=X"])

            logger.info(f"📈 Correlação Macro ({primary_ticker}/USD): {correlation:.2f}")
            return float(correlation) if not np.isnan(correlation) else 0.0
        except Exception as e:
            logger.error(f"Erro no cálculo macro: {e}")
            return 0.0

    def _calculate_backtest_benchmark(self, ticker: str) -> float:
        try:
            # Garante que o ticker existe no DF, senão usa Soja
            active_ticker = ticker if ticker in self.df_market.columns else 'ZS=F'
            series = self.df_market[active_ticker]
            
            delta = series.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rsi = 100 - (100 / (1 + (gain / loss.replace(0, 0.001))))
            
            vol = series.pct_change().rolling(window=30).std() * np.sqrt(252) * 100
            
            if rsi.iloc[-1] > 70 and vol.iloc[-1] > 40:
                return 100.0
            return 30.0
        except Exception as e:
            logger.error(f"Erro no benchmark: {e}")
            return 30.0

    def _process_contracts(self):
        logger.info(f"🔄 Processando {len(self.contracts)} contratos...")
        updates = []
        current_month = self.now_br.month

        for raw_contract in self.contracts:
            try:
                # 1. Mapeamento de Dados
                contract = {
                    "id": raw_contract.get("id"),
                    "name": raw_contract.get("client_name"),
                    "state_code": raw_contract.get("state_code", "MT"),
                    "loan_amount": raw_contract.get("loan_amount", 0),
                    "area_hectares": raw_contract.get("area_hectares", 0),
                    "estimated_yield_kg_ha": raw_contract.get("estimated_yield_kg_ha", 3600),
                    "commodity": (raw_contract.get("culture") or "soja").lower(),
                    # --- FIX: Mantemos lat/lon para o upsert não quebrar ---
                    "latitude": raw_contract.get("latitude"),
                    "longitude": raw_contract.get("longitude")
                }

                # 2. Execução do Motor
                strategy = RegionalEngineFactory.get_strategy(raw_contract)
                current_price_brl = strategy.get_soy_brl_price(self.df_market)
                
                pd_score, metrics = self.engine.calculate_pd_metrics(
                    self.df_market, 
                    contract['name'], 
                    self.df_climate, 
                    contract, 
                    current_month
                )
                
                metrics['market_price_brl'] = current_price_brl
                risk_justification = self.advisor.generate_credit_narrative(pd_score, metrics)

                # 3. Atualiza Contexto em Memória
                self.context.update_portfolio_metrics(
                    pd_score, 
                    contract['loan_amount'], 
                    metrics.get('collateral_status')
                )

                # 4. Prepara o Objeto para Salvar (CORREÇÃO AQUI)
                # O upsert precisa dos campos obrigatórios (lat/lon) mesmo que não tenham mudado
                record_to_save = {
                    "id": contract['id'],
                    "client_name": contract['name'], # Importante manter
                    "latitude": contract['latitude'], # <--- OBRIGATÓRIO
                    "longitude": contract['longitude'], # <--- OBRIGATÓRIO
                    "state_code": contract['state_code'],
                    "last_pd_score": pd_score,
                    "current_ltv": metrics.get('ltv'),
                    "collateral_status": metrics.get('collateral_status'),
                    "risk_justification": risk_justification
                }
                updates.append(record_to_save)

            except Exception as e:
                # Agora o logger estará definido aqui
                logger.error(f"❌ Erro Crítico no Contrato {raw_contract.get('id')}: {e}")

        # Salva TUDO de uma vez fora do loop
        if updates:
            logger.info(f"💾 Salvando {len(updates)} contratos em lote...")
            try:
                self.db.client.table("credit_portfolio").upsert(updates).execute()
                logger.info("✅ Lote salvo com sucesso!")
            except Exception as e:
                logger.critical(f"❌ Falha ao salvar lote no banco: {e}")

        # Salva métricas globais
        self.persister.save_market_metrics(self.df_market, self.context)

    def _load_data(self) -> bool:
        try:
            symbols = self.config.get('tickers', [])
            self.df_market = MarketLoader.get_market_data(symbols)
            dynamic_locations = [{'name': c['client_name'], 'lat': float(c['latitude']), 'lon': float(c['longitude'])} for c in self.contracts]
            self.df_climate = self.climate_intel.run_full_scan(locations=dynamic_locations)
            return True
        except Exception as e:
            logger.critical(f"Falha na ingestão: {e}", exc_info=True)
            return False

    def _extract_climate_context(self, loc_name):
        ctx = {"status_desc": "N/A", "rain_7d": 0.0, "temp_max": 0.0}
        if not self.df_climate.empty:
            c_row = self.df_climate[self.df_climate['Location'] == loc_name]
            if not c_row.empty:
                ctx = {"status_desc": str(c_row.iloc[0]['Risk_Status']), "rain_7d": float(c_row.iloc[0]['Rain_7d']), "temp_max": float(c_row.iloc[0].get('Temp_Max', 0.0))}
        return ctx