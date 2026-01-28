from .base import BaseRiskStrategy
from core.indicators.technical import TechnicalIndicators as Tech
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class MatoGrossoStrategy(BaseRiskStrategy):
    def __init__(self):
        super().__init__()
        self.region_name = "Mato Grosso"
        self.state_code = "MT" 

    def calculate_logistics_risk(self, df_market: pd.DataFrame, loc_data: dict) -> float:
        """
        Calculates logistics risk focusing on DISTANCE to penalize MT relative to PR.
        
        Logic:
        1. Base Risk = Linear distance (1 point per 50km).
        2. Multiplier = If Diesel (Oil) prices are rising, long haul costs skyrocket.
        """
        # 1. Get Distance (Default to 2000km for MT if missing)
        dist_port = loc_data.get('dist_to_port', 2000)
        
        # 2. Base Calculation (The "Descolamento" Factor)
        # MT (2000km) / 50 = 40.0 Risk Score
        # PR (500km)  / 50 = 10.0 Risk Score
        logistics_score = dist_port / 50.0

        # 3. Diesel Aggravator
        # We check Crude Oil (CL=F) as a proxy for Diesel costs
        diesel_series = df_market['CL=F']
        diesel_change = diesel_series.pct_change().iloc[-1] if len(diesel_series) > 1 else 0
        
        # If Diesel prices are rising AND the distance is long (>1000km)
        # We amplify the risk for MT, but PR (being <1000km) stays closer to base.
        if diesel_change > 0 and dist_port > 1000:
            logistics_score *= 1.5  # 50% penalty for rising fuel costs on long routes
            
        return self.sanitize_score(logistics_score)

    def calculate_climate_risk(self, df_climate: pd.DataFrame, contract_data: dict, month: int) -> float:
        # Uses 'name' mapped in pipeline
        loc_id = contract_data.get('name') 
        
        if df_climate is None or df_climate.empty:
             return 10.0 # Safe Fallback
             
        loc_row = df_climate[df_climate['Location'] == loc_id]
        
        if loc_row.empty:
            return 10.0 
        
        return float(loc_row.iloc[0]['Risk_Score'])

    def calculate_market_risk(self, df_market: pd.DataFrame) -> float:
        """
        Market Risk based on Backtest findings: RSI and Volatility.
        """
        # 1. Recover Indicators
        rsi = Tech.calculate_rsi(df_market['ZS=F'])
        vol = Tech.calculate_volatility(df_market['ZS=F'])
        
        # 2. Logic: Stretched market (High RSI) or High Volatility increases risk
        market_score = 30.0 # Base Market Risk
        
        if not pd.isna(rsi) and rsi > 70: 
            market_score += 40  # Overbought territory (High risk of correction)
            
        if not pd.isna(vol) and vol > 0.30: 
            market_score += 30 # High volatility
        
        return self.sanitize_score(market_score)

    def calculate_geopolitical_risk(self, active_alerts: list) -> float:
        """
        MT is highly sensitive to:
        1. Logistics (strikes, transport) - 100% dependent on long-haul routes
        2. War/Sanctions - Direct impact on fertilizer costs (imports)
        """
        penalty = 0.0
        
        for alert in active_alerts:
            cat = alert.get('category', '')
            level = alert.get('risk_level', 'NEUTRO')
            
            # Weight 1: Logistics (MT depends 100% on long-distance transport)
            if cat == 'GREVES_BR' or cat == 'LOGISTICA_GLOBAL':
                if level == 'CRÍTICO':
                    penalty += 20.0
                elif level == 'ALERTA':
                    penalty += 10.0
            
            # Weight 2: War/Sanctions (Direct impact on fertilizer costs)
            if cat == 'GUERRA_SANCOES':
                if level == 'CRÍTICO':
                    penalty += 15.0
                elif level == 'ALERTA':
                    penalty += 5.0

        # Cap penalty to avoid breaking the score alone
        return min(penalty, 40.0)