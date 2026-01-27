import pandas as pd
import numpy as np

class MacroIndicators:
    @staticmethod
    def calculate_currency_stress(usd_series: pd.Series) -> dict:
        """[RECUPERADO v2.8.2] Mede desvio de volatilidade cambial vs Média Histórica."""
        try:
            returns = usd_series.pct_change().dropna()
            current_vol = returns.tail(21).std() * np.sqrt(252)
            hist_vol = returns.rolling(252).std().mean() * np.sqrt(252)
            ratio = current_vol / hist_vol
            
            score = 80 if ratio > 1.5 else (40 if ratio > 1.2 else 0)
            status = "CRÍTICO" if ratio > 1.5 else ("ALERTA" if ratio > 1.2 else "ESTÁVEL")
            return {"score": score, "status": status, "ratio": round(ratio, 2)}
        except:
            return {"score": 0, "status": "NEUTRO"}

    @staticmethod
    def calculate_geopolitical_risk(gold_series: pd.Series, oil_series: pd.Series) -> dict:
        """[RECUPERADO v2.8.2] Detector de Cisne Negro (Divergência Ouro/Oil)."""
        try:
            corr = gold_series.tail(20).corr(oil_series.tail(20))
            g_ret = (gold_series.iloc[-1] / gold_series.iloc[-5]) - 1
            o_ret = (oil_series.iloc[-1] / oil_series.iloc[-5]) - 1

            if g_ret > 0.03 and o_ret > 0.05 and corr < 0.3:
                return {"score": 90, "status": "CRÍTICO (Cisne Negro Detectado)"}
            return {"score": 0, "status": "ESTÁVEL"}
        except:
            return {"score": 0, "status": "NEUTRO"}