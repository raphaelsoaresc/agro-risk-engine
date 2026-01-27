import pandas as pd
import numpy as np

class TechnicalIndicators:
    """
    Especialista em Análise Técnica (Gráficos e Estatística).
    """

    @staticmethod
    def calculate_rsi(series: pd.Series, window: int = 14) -> float:
        """Calcula RSI (Índice de Força Relativa)."""
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss.replace(0, 0.001)
        return 100 - (100 / (1 + rs)).iloc[-1]

    @staticmethod
    def calculate_volatility(series: pd.Series, window: int = 21) -> float:
        """Volatilidade anualizada (Janela de 21 dias úteis)."""
        return series.pct_change(fill_method=None).rolling(window).std().iloc[-1] * np.sqrt(252)

    @staticmethod
    def analyze_trend(series: pd.Series, short_window=9, long_window=21) -> str:
        """Tendência via Cruzamento de Médias (EMA)."""
        ema_short = series.ewm(span=short_window, adjust=False).mean().iloc[-1]
        ema_long = series.ewm(span=long_window, adjust=False).mean().iloc[-1]
        if ema_short > ema_long: return "ALTA"
        elif ema_short < ema_long: return "BAIXA"
        return "LATERAL"