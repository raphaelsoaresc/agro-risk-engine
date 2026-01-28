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

    def calculate_geopolitical_risk(self, active_alerts: list) -> float:
        """
        PR has better logistics and strong cooperatives, so:
        1. Lower impact from strikes (half of MT's weight)
        2. Sensitive to global extreme climate (affects commodity prices)
        """
        penalty = 0.0
        
        for alert in active_alerts:
            cat = alert.get('category', '')
            level = alert.get('risk_level', 'NEUTRO')
            
            # Weight 1: Logistics (Lower impact than MT)
            if cat == 'GREVES_BR':
                if level == 'CRÍTICO':
                    penalty += 10.0  # Half of MT's weight
                elif level == 'ALERTA':
                    penalty += 5.0
            
            # Weight 2: Global Extreme Climate (Affects commodity prices)
            if cat == 'CLIMA_EXTREMO':
                if level == 'CRÍTICO':
                    penalty += 10.0

        return min(penalty, 30.0)