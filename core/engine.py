import pandas as pd
import numpy as np
from datetime import datetime
from core.indicators.technical import TechnicalIndicators as Tech
from core.indicators.fundamental import FundamentalIndicators as Fund
from core.indicators.macro import MacroIndicators as Macro
from core.seasonality import SeasonalityManager # Importa a inteligência existente
import logging

logger = logging.getLogger(__name__)

class RiskEngine:
    def __init__(self):
        # Inicializa o gestor de sazonalidade por Estado
        from core.seasonality import SeasonalityManager
        self.seasonality = SeasonalityManager() # Instancia o gestor de safras

    def _sanitize_metrics(self, data):
        if isinstance(data, dict): return {k: self._sanitize_metrics(v) for k, v in data.items()}
        elif isinstance(data, float):
            return 0.0 if np.isnan(data) or np.isinf(data) else round(data, 4)
        return data

    def _is_data_stale(self, df: pd.DataFrame) -> bool:
        if df.empty: return True
        if len(df) >= 3:
            if df['ZS=F'].tail(3).std() == 0 or df['USDBRL=X'].tail(3).std() == 0:
                return True
        last_update = pd.to_datetime(df.index[-1])
        return (datetime.now() - last_update.replace(tzinfo=None)).days > 2

    def _calculate_calibrated_market_score(self, soy_series, usd_series):
        """
        [CORRIGIDO] Lógica calibrada: Queda de preço não é Risco 80.
        """
        # 1. Volatilidade
        soy_vol = Tech.calculate_volatility(soy_series)
        vol_score = min((soy_vol * 100) * 3, 100)
        
        # 2. Tendência (CORRIGIDO AQUI)
        trend = Tech.analyze_trend(soy_series)
        # Reduzimos drasticamente as pontuações. Queda de preço é normal.
        if trend == "STRONG_DOWN": price_risk = 40 # Antes era 80
        elif trend == "DOWN": price_risk = 25      # Antes era 60
        elif trend == "NEUTRAL": price_risk = 10   # Antes era 40
        else: price_risk = 5 
        
        # 3. Câmbio
        usd_trend = Tech.analyze_trend(usd_series)
        fx_hedge = -20 if "UP" in usd_trend else 0
        
        final_score = (vol_score * 0.3) + (price_risk * 0.7) + fx_hedge
        return max(0, min(final_score, 100))

    def calculate_full_analysis(self, df_market, loc_name, df_climate=None, month=None):
        # Verificação de integridade de colunas
        if 'ZS=F' not in df_market.columns or 'USDBRL=X' not in df_market.columns:
            logger.warning("⚠️ Dados de mercado incompletos. Retornando análise neutra.")
            return self._get_empty_analysis()  # Retorna scores neutros em vez de quebrar

        stale = self._is_data_stale(df_market)
        soy = df_market['ZS=F']
        usd = df_market['USDBRL=X']
        
        trend = Tech.analyze_trend(soy) 
        geo = Macro.calculate_geopolitical_risk(df_market.get('GC=F', soy), df_market.get('CL=F', soy))
        stress = Macro.calculate_currency_stress(usd)
        
        raw_basis = Fund.calculate_basis_proxy(
            Tech.calculate_volatility(soy), 
            usd.pct_change().iloc[-1], 
            "NEUTRO", 
            stale,
            trend 
        )

        seasonality_factor = 0.8 if month in [3, 4] else 0.2
        score_logistica = min(100, raw_basis * (1 + (seasonality_factor ** 2)))

        china = Fund.calculate_china_demand(soy, df_market.get('HE=F'))
        
        climate_score = 10 
        climate_lvl = "NORMAL"
        
        if df_climate is not None and not df_climate.empty:
            loc_climate = df_climate[df_climate['Location'] == loc_name]
            if not loc_climate.empty:
                climate_score = float(loc_climate.iloc[0]['Risk_Score'])
                climate_lvl = loc_climate.iloc[0]['Risk_Status']

        # Usa a nova função calibrada
        market_score = self._calculate_calibrated_market_score(soy, usd)
        
        washout = Fund.calculate_washout_probability(climate_lvl, trend, soy.pct_change(30).iloc[-1])
        
        results = {
            "Mercado": market_score,
            "Câmbio": min(100, (Tech.calculate_rsi(usd)*0.5) + (stress['score']*0.5)),
            "Logística": score_logistica,
            "Clima": climate_score 
        }
        
        metrics = {
            "washout_risk": washout,
            "china_demand": china,
            "geopolitics": geo,
            "is_stale": stale,
            "basis_status": f"Basis {loc_name}: {'Estressado' if score_logistica > 60 else 'Normal'}"
        }
        
        return self._sanitize_metrics(results), self._sanitize_metrics(metrics)
    
    class OpportunityEngine:
        @staticmethod
        def analyze_profit_windows(df_market, fai_status):
            opportunities = []
            if "OPORTUNIDADE" in str(fai_status).upper():
                opportunities.append("💰 BARTER: Relação de troca favorável.")
            soy_rsi = Tech.calculate_rsi(df_market['ZS=F'])
            if soy_rsi < 30:
                opportunities.append(f"📈 TÉCNICO: Soja em sobrevenda (RSI {soy_rsi:.0f}).")
            usd_rsi = Tech.calculate_rsi(df_market['USDBRL=X'])
            if usd_rsi < 30:
                opportunities.append("🚢 LOGÍSTICA: Dólar em baixa.")
            return opportunities

    def calculate_market_score(self, ticker: str, df_market: pd.DataFrame) -> float:
        """
        Calculate the market risk score for a given ticker.
        """
        try:
            if ticker not in df_market.columns:
                raise ValueError(f"Ticker {ticker} not found in market data.")

            # Calculate volatility
            vol_30d = df_market[ticker].pct_change().rolling(window=30).std() * np.sqrt(252) * 100
            current_vol = vol_30d.iloc[-1]

            # Soybean-specific logic (e.g., Crush Margin)
            if ticker in ["ZS=F", "ZM=F", "ZL=F"]:  # Soybean, Soybean Meal, Soybean Oil
                crush_margin = self._calculate_crush_margin(df_market)
                logger.info(f"Crush Margin for {ticker}: {crush_margin:.2f}")
                # Adjust score based on crush margin
                if crush_margin < 50:
                    return min(current_vol * 1.2, 100)  # Higher risk if margin is low
                else:
                    return min(current_vol, 100)

            # General logic for other commodities (e.g., Wheat, Corn)
            logger.info(f"Volatility for {ticker}: {current_vol:.2f}")
            if current_vol > 40:  # High volatility threshold
                return 100.0
            elif current_vol > 20:  # Moderate volatility
                return 50.0
            else:
                return 20.0  # Low volatility

        except Exception as e:
            logger.error(f"Error calculating market score for {ticker}: {e}")
            return 0.0

    def _calculate_crush_margin(self, df_market: pd.DataFrame) -> float:
        """
        Calculate the soybean crush margin based on Soybean, Soybean Meal, and Soybean Oil prices.
        """
        try:
            if not all(t in df_market.columns for t in ["ZS=F", "ZM=F", "ZL=F"]):
                raise ValueError("Missing data for crush margin calculation.")

            # Example formula: Crush Margin = (Meal + Oil) - Soybean
            meal_price = df_market["ZM=F"].iloc[-1]
            oil_price = df_market["ZL=F"].iloc[-1]
            soybean_price = df_market["ZS=F"].iloc[-1]
            crush_margin = (meal_price + oil_price) - soybean_price
            return crush_margin

        except Exception as e:
            logger.error(f"Error calculating crush margin: {e}")
            return 0.0

    def calculate_pd_metrics(self, df_market, loc_name, df_climate, contract_data, month):
        """
        Calcula a Probabilidade de Default (PD) com Regra de Veto Climático.
        """
        # 1. Risco Produtivo (Safra)
        raw_scores, metrics = self.calculate_full_analysis(df_market, loc_name, df_climate, month)
        
        # Pesos Base: Clima (40%) + Logística (30%) + Mercado (20%) + Câmbio (10%)
        productive_pd = (
            (raw_scores.get('Clima', 0) * 0.4) +
            (raw_scores.get('Logística', 0) * 0.3) +
            (raw_scores.get('Mercado', 0) * 0.2) +
            (raw_scores.get('Câmbio', 0) * 0.1)
        )
        
        # 2. Risco Comportamental
        serasa = contract_data.get('credit_score_serasa', 700)
        dti = contract_data.get('debt_to_income_ratio', 0.3)
        
        # Normaliza Serasa (0 a 1000 -> Risco 100 a 0)
        behavioral_risk = (1000 - serasa) / 10 
        # Penaliza DTI alto (se DTI > 0.5, adiciona risco exponencial)
        if dti > 0.5:
            behavioral_risk += (dti * 40) # Penalidade agressiva por alavancagem
        
        # 3. PD FINAL (Híbrida com GATILHO DE CATAÁSTROFE)
        
        climate_score = raw_scores.get('Clima', 0)
        
        # --- AQUI ESTÁ A MUDANÇA (REGRA DE VETO) ---
        if climate_score > 80:
            # Se o clima destruiu a safra, o comportamento importa pouco (10%)
            # O risco produtivo domina (90%) e ganha um boost de severidade
            final_pd = (productive_pd * 0.9) + (behavioral_risk * 0.1)
            final_pd = final_pd * 1.2 # Boost de Pânico
        elif climate_score > 50:
            # Situação de Alerta
            final_pd = (productive_pd * 0.7) + (behavioral_risk * 0.3)
        else:
            # Situação Normal (Comportamento do cliente pesa mais)
            final_pd = (productive_pd * 0.4) + (behavioral_risk * 0.6)
            
        # Teto de 99.9%
        final_pd = min(final_pd, 99.9)
        
        # --- FIM DA MUDANÇA ---

        # Cálculo de LTV Estressado
        current_price_brl = metrics.get('market_price_brl', 120.0)
        
        credit_metrics = self._calculate_ltv_exposure(
            contract_data, 
            climate_score, # Passa o score climático puro para o LTV
            current_price_brl,
            month
        )
        
        metrics.update(credit_metrics)
        return round(final_pd, 2), metrics

    def _calculate_dynamic_lgd(self, exposure, collateral_value):
        """
        Calcula a Loss Given Default baseada na cobertura de garantia.
        Padrão Institucional: Considera custos de execução (Haircut).
        """
        if exposure <= 0: return 0.0
        
        # Haircut de Execução (Advogados, Leilão, Liquidez da Terra)
        # Terras no MT têm liquidez alta (Haircut 20%)
        # Terras em fronteira agrícola têm liquidez baixa (Haircut 40%)
        execution_haircut = 0.25 # Conservador (25%)
        
        recoverable_amount = collateral_value * (1 - execution_haircut)
        
        # Se a garantia cobre tudo, LGD é zero (ou um piso técnico de 5%)
        if recoverable_amount >= exposure:
            return 0.05 # Piso técnico operacional
            
        # Cálculo da perda residual
        loss = exposure - recoverable_amount
        lgd = loss / exposure
        
        return min(lgd, 1.0) # Teto de 100%

    def _calculate_ltv_exposure(self, contract, climate_score, current_price_brl, month):
        """
        Cálculo de LTV com Sensibilidade Fenológica por Estado.
        """
        loan_amount = float(contract.get('loan_amount', 0))
        area = float(contract.get('area_hectares', 0))
        initial_yield = float(contract.get('estimated_yield_kg_ha', 3600))
        state_code = contract.get('state_code', 'MT')

        if loan_amount <= 0 or area <= 0:
            return {"ltv": 0, "collateral_status": "DATA_MISSING"}

        # Recupera o peso fenológico específico do Estado e Mês
        pheno_weight = self.seasonality.get_state_weight(month, state_code)
        
        # Impacto climático calibrado: (Score * Sensibilidade Base) * Peso do Estado/Mês
        yield_reduction_factor = (climate_score * 0.005) * pheno_weight
        stressed_yield = initial_yield * (1 - yield_reduction_factor)
        
        # Valor da Garantia (Safra Estressada * Área * Preço Saca)
        total_collateral_value = (stressed_yield * area) * (current_price_brl / 60)
        
        ltv = (loan_amount / total_collateral_value) if total_collateral_value > 0 else 999
        
        # Status Institucional de Garantia
        status = "HEALTHY"
        if ltv > 0.85: status = "WARNING"
        if ltv > 1.0: status = "CRITICAL_UNCOVERED"

        return {
            "ltv": round(ltv, 4),
            "collateral_value_brl": round(total_collateral_value, 2),
            "collateral_status": status,
            "yield_loss_est": f"{yield_reduction_factor:.1%}",
            "pheno_weight_applied": pheno_weight
        }
    
    def _get_empty_analysis(self):
        """Retorna uma análise neutra para evitar falhas."""
        results = {
            "Mercado": 0.0,
            "Câmbio": 0.0,
            "Logística": 0.0,
            "Clima": 0.0
        }
        metrics = {
            "washout_risk": 0.0,
            "china_demand": 0.0,
            "geopolitics": 0.0,
            "is_stale": True,
            "basis_status": "N/A"
        }
        return results, metrics