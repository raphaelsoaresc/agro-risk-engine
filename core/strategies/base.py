from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
import yaml

class BaseRiskStrategy(ABC):
    def __init__(self):
        self.region_name = "Base"
        self.settings = self._load_settings()

    def _load_settings(self) -> dict:
        """
        Carrega as configurações do arquivo settings.yaml.
        """
        try:
            with open("/home/obscuritenoir/Portfolio/credit-risk-engine/settings.yaml", "r") as f:
                return yaml.safe_load(f)
        except Exception as e:
            raise RuntimeError(f"Erro ao carregar settings.yaml: {e}")

    def get_data_source(self, ticker: str) -> str:
        """
        Retorna a fonte de dados para um ticker específico com base nas configurações.
        """
        overrides = self.settings.get("market_sources", {}).get("overrides", {})
        return overrides.get(ticker, self.settings.get("market_sources", {}).get("default", "yahoo"))

    def translate_ticker(self, ticker: str, source: str) -> str:
        """
        Traduz o ticker para o formato esperado pela fonte de dados.
        """
        translation_map = self.settings.get("ticker_translation", {}).get(source, {})
        return translation_map.get(ticker, ticker)

    @abstractmethod
    def calculate_logistics_risk(self, df_market: pd.DataFrame, contract_data: dict) -> float:
        """
        Calcula o risco logístico com base nos dados de mercado e contrato.

        Parâmetros:
        - df_market: DataFrame contendo os dados de mercado.
        - contract_data: Dicionário que deve conter a chave 'dist_to_port', representando a distância ao porto.

        Retorna:
        - Risco logístico como um valor float.
        """
        pass

    @abstractmethod
    def calculate_climate_risk(self, df_climate: pd.DataFrame, contract_data: dict, month: int) -> float:
        """
        Calcula o risco climático com base nos dados climáticos, contrato e mês.

        Parâmetros:
        - df_climate: DataFrame contendo os dados climáticos.
        - contract_data: Dicionário que deve conter a chave 'identifier', que mapeia para 'Location' no df_climate.
        - month: Inteiro representando o mês para o qual o risco climático será calculado.

        Retorna:
        - Risco climático como um valor float.
        """
        pass

    @abstractmethod
    def calculate_market_risk(self, df_market: pd.DataFrame) -> float:
        pass

    def get_soy_brl_price(self, df_market: pd.DataFrame) -> float:
        """
        Retorna o preço da saca de 60kg em BRL.
        Base institucional para cálculo de valor de garantia.
        """
        try:
            soy_chicago = df_market['ZS=F'].iloc[-1] 
            usd_brl = df_market['USDBRL=X'].iloc[-1]
            
            # Conversão: (Cents/Bushel / 100) * USD * 2.2046 (Bushels to 60kg Bag)
            price_saca = (soy_chicago / 100) * usd_brl * 2.2046
            return round(float(price_saca), 2)
        except:
            return 0.0

    def sanitize_score(self, score: float) -> float:
        """Garante que o score não saia do range 0-100"""
        return float(np.clip(score, 0.0, 100.0))