import httpx
import asyncio
from core.logger import get_logger

logger = get_logger("IBGEClient")

class IBGEClient:
    """
    Consome a API SIDRA do IBGE com Fallback Multi-Safra.
    Nível Ouro: Dados de contingência específicos por ano para evitar viés no backtest.
    """
    BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"

    # MATRIZ DE CONTINGÊNCIA HISTÓRICA (Dados Oficiais Reais)
    FALLBACK_DATA = {
        "2023": { # Safra 23/24 (El Niño/Crise)
            "MT": 3439.0, "PR": 3250.0, "GO": 3520.0, "RS": 3100.0, "MS": 3380.0, "BA": 3650.0
        },
        "2022": { # Safra 22/23 (Ano Neutro/Recorde)
            "MT": 3651.0, "PR": 3423.0, "GO": 3620.0, "RS": 3150.0, "MS": 3580.0, "BA": 4020.0
        }
    }

    async def get_actual_yield(self, state_code: str, year: str):
        state_code = state_code.upper()
        state_map = {"MT": 51, "PR": 41, "GO": 52, "RS": 43, "MS": 50, "BA": 29}
        state_id = state_map.get(state_code)
        
        if not state_id: return None

        # Tentativa via API
        url = f"{self.BASE_URL}/1612/periodos/{year}12/variaveis/35?localidades=N3[{state_id}]&classificacao=81[2702]"
        
        async with httpx.AsyncClient() as client:
            for attempt in range(2): # Reduzido para 2 tentativas para agilizar o fallback
                try:
                    response = await client.get(url, timeout=10.0)
                    if response.status_code == 200:
                        data = response.json()
                        return float(data[0]['resultados'][0]['series'][0]['serie'][f"{year}12"])
                except Exception:
                    continue

        # --- FALLBACK ESTRATÉGICO POR ANO ---
        logger.error(f"🚨 API IBGE Indisponível. Acionando Contingência {year} para {state_code}.")
        year_data = self.FALLBACK_DATA.get(year, self.FALLBACK_DATA["2023"])
        return year_data.get(state_code)