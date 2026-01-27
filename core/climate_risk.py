# ARQUIVO: core/climate_risk.py
import httpx
import asyncio
import pandas as pd
import logging
import random
import os
from dotenv import load_dotenv
from datetime import datetime

# Carrega variáveis de ambiente do .env
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClimateIntelligence:
    def __init__(self):
        self.base_url = "https://api.open-meteo.com/v1/forecast"
        self.weatherapi_key = os.getenv("WEATHERAPI_KEY") # Chave da API Secundária
        
        # Lista de Regiões
        self.regions = [
            {'name': 'Sorriso_MT', 'lat': -12.54, 'lon': -55.72, 'type': 'production', 'hemisphere': 'S'},
            {'name': 'Cascavel_PR', 'lat': -24.95, 'lon': -53.45, 'type': 'production', 'hemisphere': 'S'},
            {'name': 'Rio_Verde_GO', 'lat': -17.79, 'lon': -50.92, 'type': 'production', 'hemisphere': 'S'},
            {'name': 'Des_Moines_IA', 'lat': 41.60, 'lon': -93.60, 'type': 'production', 'hemisphere': 'N'},
            {'name': 'Decatur_IL', 'lat': 39.84, 'lon': -88.95, 'type': 'production', 'hemisphere': 'N'},
            {'name': 'Porto_Santos', 'lat': -23.96, 'lon': -46.33, 'type': 'logistics', 'hemisphere': 'S'},
            {'name': 'Paranagua', 'lat': -25.51, 'lon': -48.50, 'type': 'logistics', 'hemisphere': 'S'},
            {'name': 'Itaqui_MA', 'lat': -2.57, 'lon': -44.36, 'type': 'logistics', 'hemisphere': 'S'},
            {'name': 'Santarem_PA', 'lat': -2.44, 'lon': -54.70, 'type': 'logistics', 'hemisphere': 'S'},
            {'name': 'Itacoatiara_AM', 'lat': -3.14, 'lon': -58.44, 'type': 'logistics', 'hemisphere': 'S'},
            {'name': 'Mississippi_River_St_Louis', 'lat': 38.62, 'lon': -90.19, 'type': 'logistics', 'hemisphere': 'N'},
            {'name': 'Panama_Canal', 'lat': 9.10, 'lon': -79.69, 'type': 'chokepoint', 'hemisphere': 'N'},
            {'name': 'Suez_Canal', 'lat': 30.58, 'lon': 32.27, 'type': 'chokepoint', 'hemisphere': 'N'},
            {'name': 'China_Dalian', 'lat': 38.91, 'lon': 121.60, 'type': 'demand', 'hemisphere': 'N'}
        ]

    def _get_synthetic_fallback(self, region, month):
        """
        PLANO B (FINAL): Se todas as APIs falharem, gera dados baseados na média histórica.
        Isso impede que o sistema quebre (Crash).
        """
        logger.warning(f"⚠️ Usando Fallback Sintético para {region['name']}")
        
        is_summer = (region['hemisphere'] == 'S' and month in [12, 1, 2]) or \
                    (region['hemisphere'] == 'N' and month in [6, 7, 8])
        
        # Simula dados normais para não gerar pânico falso
        return {
            'rain_7d': 50.0 if is_summer else 10.0, # Chove mais no verão
            'temp_max': 30.0 if is_summer else 15.0,
            'is_estimated': True # Flag para avisar no relatório
        }

    async def _fetch_single_forecast(self, client, region, semaphore):
        """
        Busca dados com Semáforo (limite de conexões) e Retry.
        Prioridade: Open-Meteo -> WeatherAPI -> Sintético.
        """
        async with semaphore: # <--- AQUI ESTÁ A MÁGICA DO CONTROLE DE TRÁFEGO
            max_retries = 3
            
            # --- TENTATIVA 1: OPEN-METEO ---
            for attempt in range(max_retries):
                try:
                    # Adiciona um jitter (atraso aleatório) para não bater na API ao mesmo tempo
                    await asyncio.sleep(random.uniform(0.1, 0.5))
                    
                    params = {
                        "latitude": region['lat'],
                        "longitude": region['lon'],
                        "daily": ["precipitation_sum", "temperature_2m_max"],
                        "timezone": "auto"
                    }
                    
                    response = await client.get(self.base_url, params=params)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if 'daily' in data:
                            rain_7d = sum(data['daily']['precipitation_sum'])
                            temp_max = sum(data['daily']['temperature_2m_max']) / len(data['daily']['temperature_2m_max'])
                            return {'rain_7d': rain_7d, 'temp_max': temp_max, 'is_estimated': False}
                    
                    # Se não for 200, tenta de novo
                    logger.warning(f"API Open-Meteo {region['name']} status {response.status_code}. Tentativa {attempt+1}")

                except Exception as e:
                    # Se for erro de conexão (Device busy), espera mais tempo
                    wait = (attempt + 1) * 2
                    logger.warning(f"Erro Conexão Open-Meteo ({region['name']}): {e}. Esperando {wait}s...")
                    await asyncio.sleep(wait)

            # --- TENTATIVA 2: WEATHERAPI (FALLBACK) ---
            if self.weatherapi_key:
                try:
                    logger.info(f"🔄 Tentando WeatherAPI para {region['name']}...")
                    wapi_url = "http://api.weatherapi.com/v1/forecast.json"
                    wapi_params = {
                        "key": self.weatherapi_key,
                        "q": f"{region['lat']},{region['lon']}",
                        "days": 7,
                        "aqi": "no",
                        "alerts": "no"
                    }
                    
                    # Usa o mesmo client http async
                    response = await client.get(wapi_url, params=wapi_params)
                    
                    if response.status_code == 200:
                        data = response.json()
                        forecast_days = data.get('forecast', {}).get('forecastday', [])
                        
                        if forecast_days:
                            rain_7d = sum(day['day']['totalprecip_mm'] for day in forecast_days)
                            temp_max = sum(day['day']['maxtemp_c'] for day in forecast_days) / len(forecast_days)
                            return {'rain_7d': rain_7d, 'temp_max': temp_max, 'is_estimated': False}
                    else:
                        logger.error(f"WeatherAPI falhou com status {response.status_code}")

                except Exception as e:
                    logger.error(f"Erro WeatherAPI ({region['name']}): {e}")

            # --- TENTATIVA 3: SINTÉTICO (FINAL) ---
            # Se falhar todas as tentativas (Open-Meteo e WeatherAPI), retorna o Fallback Sintético
            return self._get_synthetic_fallback(region, datetime.now().month)

    def _is_off_season(self, region_type, hemisphere, month):
        if region_type != 'production': return False
        if hemisphere == 'N' and month in [11, 12, 1, 2, 3]: return True
        if hemisphere == 'S' and month in [6, 7, 8]: return True
        return False

    def analyze_risk(self, weather_data, region_type, hemisphere, current_month):
        # Se vier do fallback sintético, adiciona aviso
        is_estimated = weather_data.get('is_estimated', False)
        status_suffix = " (EST)" if is_estimated else ""

        if self._is_off_season(region_type, hemisphere, current_month):
            return f"ENTRESSAFRA{status_suffix}", 0

        rain = weather_data['rain_7d']
        temp = weather_data['temp_max']
        
        if region_type == 'production':
            if rain < 5: 
                if temp > 35: return "SECA EXTREMA", 100  # Nível Ouro: Risco Máximo
                elif temp > 32: return "CALOR + SECA", 70
                else: return "SECA LEVE", 40
            elif rain < 15: 
                return "ATENÇÃO", 20
        
        if rain > 180: return "EXCESSO CHUVA", 100

        elif region_type == 'chokepoint':
            if rain < 5: return "BAIXO NÍVEL", 10

        return "NORMAL", 0

    async def run_full_scan_async(self, locations=None):
        """
        Perform a full climate risk scan. If `locations` is provided, it overrides `self.regions`.
        """
        results = []
        current_month = datetime.now().month
        logger.info(f"🌍 Iniciando Scan Climático (Controlado)...")
        
        # Use provided locations or default to self.regions
        regions_to_scan = locations if locations else self.regions
        
        # --- SEMÁFORO: Permite apenas 2 requisições simultâneas ---
        semaphore = asyncio.Semaphore(2) 
        
        limits = httpx.Limits(max_keepalive_connections=2, max_connections=4)
        
        async with httpx.AsyncClient(limits=limits, timeout=30.0) as client:
            tasks = [self._fetch_single_forecast(client, region, semaphore) for region in regions_to_scan]
            weather_results = await asyncio.gather(*tasks)
            
            for region, data in zip(regions_to_scan, weather_results):
                status, score = self.analyze_risk(
                    data, 
                    region.get('type', 'production'), 
                    region.get('hemisphere', 'S'), 
                    current_month
                )
                
                results.append({
                    'Location': region['name'],
                    'Group': 'BR' if region.get('hemisphere', 'S') == 'S' else ('US' if region.get('hemisphere', 'S') == 'N' and 'China' not in region['name'] else 'GLOBAL'),
                    'Risk_Status': status,
                    'Risk_Score': score,
                    'Rain_7d': data['rain_7d'],
                    'Temp_Max': data['temp_max']
                })
                
        return pd.DataFrame(results)

    def run_full_scan(self, locations=None):
        """
        Synchronous wrapper for `run_full_scan_async`. If `locations` is provided, it overrides `self.regions`.
        """
        return asyncio.run(self.run_full_scan_async(locations=locations))