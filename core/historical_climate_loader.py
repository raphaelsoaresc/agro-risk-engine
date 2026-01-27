import httpx
import asyncio
import hashlib
import pandas as pd
from datetime import datetime
from core.db import DatabaseManager
from core.logger import get_logger

logger = get_logger("HistoricalClimateLoader")

class HistoricalClimateLoader:
    """
    Carregador de Clima Histórico Real (Open-Meteo Archive).
    Responsável por popular o cache com dados climáticos VERDADEIROS da safra passada.
    """
    
    # API de Arquivo (Dados passados reais, não previsão)
    ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

    def __init__(self, db_manager):
        self.db = db_manager
        # Semáforo para não estourar o rate limit da API Open-Meteo
        self.semaphore = asyncio.Semaphore(5) 

    def _generate_hash(self, lat, lon, start, end):
        """Gera ID único para o cache."""
        # Normaliza datas se vierem como datetime
        if isinstance(start, datetime): start = start.strftime('%Y-%m-%d')
        if isinstance(end, datetime): end = end.strftime('%Y-%m-%d')
        
        content = f"{lat:.2f}_{lon:.2f}_{start}_{end}"
        return hashlib.md5(content.encode()).hexdigest()

    async def fetch_real_history(self, lat, lon, start_date, end_date):
        """
        Busca a verdade climática histórica para um ponto específico.
        """
        # Garante formato string YYYY-MM-DD
        s_date = start_date.strftime('%Y-%m-%d') if isinstance(start_date, datetime) else start_date
        e_date = end_date.strftime('%Y-%m-%d') if isinstance(end_date, datetime) else end_date

        cache_hash = self._generate_hash(lat, lon, s_date, e_date)
        
        # 1. Check Cache (Evita re-download)
        # Usamos a tabela 'climate_historical_cache' que o BacktestEngine já sabe ler
        cached = self.db.client.table("climate_historical_cache")\
            .select("data_json")\
            .eq("coordinate_hash", cache_hash)\
            .execute()
        
        if cached.data:
            # logger.info(f"📦 Cache Hit Clima: {lat}, {lon}")
            return pd.DataFrame(cached.data[0]['data_json'])

        # 2. Fetch API Real (Com controle de concorrência)
        async with self.semaphore:
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": s_date,
                "end_date": e_date,
                "daily": ["precipitation_sum", "temperature_2m_max"],
                "timezone": "America/Sao_Paulo"
            }
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                try:
                    resp = await client.get(self.ARCHIVE_URL, params=params)
                    
                    if resp.status_code != 200:
                        logger.warning(f"⚠️ Falha Open-Meteo ({resp.status_code}) para {lat},{lon}")
                        return pd.DataFrame()

                    data = resp.json()
                    
                    # Processamento para formato tabular
                    if 'daily' not in data:
                        return pd.DataFrame()

                    df = pd.DataFrame({
                        'date': data['daily']['time'],
                        'precipitation': data['daily']['precipitation_sum'],
                        'temp_max': data['daily']['temperature_2m_max']
                    })
                    
                    # 3. Persistência (Cache)
                    self.db.client.table("climate_historical_cache").upsert({
                        "coordinate_hash": cache_hash,
                        "latitude": lat,
                        "longitude": lon,
                        "data_json": df.to_dict(orient='records')
                    }, on_conflict="coordinate_hash").execute()
                    
                    logger.info(f"✅ Clima Real Baixado: {lat:.2f}, {lon:.2f} ({len(df)} dias)")
                    return df

                except Exception as e:
                    logger.error(f"❌ Erro API Clima: {e}")
                    return pd.DataFrame()

    async def batch_load(self, contracts, start_date, end_date):
        """
        Método exigido pelo run_backtest.py.
        Carrega dados para todos os contratos de forma eficiente.
        """
        logger.info(f"🌍 Iniciando carga em lote de clima real para {len(contracts)} contratos...")
        
        # 1. Deduplicação de Coordenadas (Muitos contratos podem estar na mesma fazenda/região)
        unique_coords = set()
        for c in contracts:
            # Arredonda para 2 casas para agrupar vizinhos próximos e economizar API
            lat = round(float(c['latitude']), 2)
            lon = round(float(c['longitude']), 2)
            unique_coords.add((lat, lon))
            
        logger.info(f"📍 Locais únicos identificados: {len(unique_coords)}")

        # 2. Criação das Tarefas Assíncronas
        tasks = []
        for lat, lon in unique_coords:
            tasks.append(self.fetch_real_history(lat, lon, start_date, end_date))
            
        # 3. Execução Paralela
        await asyncio.gather(*tasks)
        logger.info("✅ Carga de Clima Histórico concluída.")