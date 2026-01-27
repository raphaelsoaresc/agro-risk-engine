from .base import BaseRiskStrategy
from core.indicators.technical import TechnicalIndicators as Tech
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class ParanaStrategy(BaseRiskStrategy):
    def __init__(self):
        super().__init__()
        self.region_name = "Paraná"
        self.state_code = "PR" # Vinculação explícita

    def calculate_logistics_risk(self, df_market: pd.DataFrame, loc_data: dict) -> float:
        # PR tem custo logístico menor e mais estável
        return self.sanitize_score(15.0)

    def calculate_climate_risk(self, df_climate: pd.DataFrame, contract_data: dict, month: int) -> float:
        # Agora contract_data['name'] sempre existirá por causa do Mapper no Pipeline
        loc_id = contract_data.get('name') 
        loc_row = df_climate[df_climate['Location'] == loc_id]
        
        if loc_row.empty:
            return 10.0 # Fallback seguro
        
        return float(loc_row.iloc[0]['Risk_Score'])

    def calculate_market_risk(self, df_market: pd.DataFrame) -> float:
        # 1. Recuperamos os indicadores que o backtest provou que funcionam
        soy_brl = self.get_soy_brl_price(df_market) # Preço em R$
        rsi = Tech.calculate_rsi(df_market['ZS=F'])
        vol = Tech.calculate_volatility(df_market['ZS=F'])
        
        # 2. Lógica do Backtest: Se o mercado está esticado (RSI alto) 
        # ou volátil demais, o risco de mercado sobe.
        market_score = 30.0 # Base - Mantendo alinhado com o MT base
        
        if rsi > 70: market_score += 40  # Sobrecompra (Risco de queda)
        if vol > 0.30: market_score += 30 # Alta volatilidade
        
        return self.sanitize_score(market_score)