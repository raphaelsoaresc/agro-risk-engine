# core/indicators/fundamental.py
import pandas as pd
import numpy as np

class FundamentalIndicators:
    @staticmethod
    def calculate_washout_probability(climate_risk_level, price_trend, price_change_30d):
        """[RECUPERADO v2.8.2] Calcula risco de default/quebra de contrato."""
        try:
            risk_score = 0
            # Fator 1: Risco Biológico (Quebra de Safra)
            if climate_risk_level == "CRÍTICO": risk_score += 60
            elif climate_risk_level == "ALERTA": risk_score += 20
                
            # Fator 2: Incentivo Econômico (Default Estratégico)
            if price_change_30d > 0.10 and price_trend == "ALTA": risk_score += 40
            elif price_trend == "ALTA": risk_score += 15
            elif price_trend == "BAIXA": risk_score -= 10
                
            status = "BAIXO"
            if risk_score >= 80: status = "MÁXIMO (Risco de Default Iminente)"
            elif risk_score >= 50: status = "ALTO (Monitorar Contrapartes)"
            elif risk_score >= 20: status = "MODERADO"
            
            return {"score": max(0, min(100, risk_score)), "status": status}
        except:
            return {"score": 0, "status": "DADOS INSUFICIENTES"}

    @staticmethod
    def calculate_china_demand(soy_series, hog_series):
        """[FIX: DYNAMIC BASE] Suavização para evitar viés de ponto único."""
        try:
            base_soy = soy_series.head(20).mean()
            base_hog = hog_series.head(20).mean()
            soy_norm = soy_series / base_soy * 100
            hog_norm = hog_series / base_hog * 100
            margin_proxy = hog_norm.iloc[-1] - soy_norm.iloc[-1]
            
            if margin_proxy < -15: return {"score": 90, "status": "BAIXA (Risco Washout)"}
            return {"score": 20, "status": "NORMAL"}
        except:
            return {"score": 0, "status": "DADOS INSUFICIENTES"}

    @staticmethod
    def calculate_basis_proxy(volatility, usd_ret, china_demand, is_stale, price_trend="LATERAL"):
        """[RECUPERADO v2.8.2] Basis Sintético com Filtro Direcional."""
        fx_pressure = abs(usd_ret) if usd_ret < -0.5 else 0
        stale_penalty = 2.0 if is_stale else 1.0
        vol_multiplier = 0.5 if price_trend == "BAIXA" else 1.5
        china_factor = 20 if "ALTA" in str(china_demand) else (-10 if "BAIXA" in str(china_demand) else 0)
        risk_score = ((volatility * 100 * vol_multiplier) + (fx_pressure * 3.0) + china_factor) * stale_penalty
        return min(100, risk_score)