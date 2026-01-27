import yfinance as yf
import pandas as pd
import requests
from datetime import datetime
from core.logger import get_logger

logger = get_logger(__name__)

def get_data():
    """
    VISÃO ESTRATÉGICA: Coleta de indicadores financeiros e gargalos logísticos globais.
    """
    month = datetime.now().month
    logger.info("Iniciando coleta de dados globais", extra={"context": "destination"})
    
    # 1. FINANCEIRO (Moedas de Importação/Exportação)
    risk_metrics = {"eur_ret": None, "cny_ret": None}
    
    try:
        tickers = ['EURUSD=X', 'CNY=X']
        # Timeout e tratamento de dados vazios para resiliência
        data = yf.download(tickers, period='5d', progress=False, timeout=10)['Close']
        
        if not data.empty and len(data) > 1:
            # pct_change(fill_method=None) evita avisos de depreciação do pandas
            returns = data.pct_change(fill_method=None).iloc[-1]
            risk_metrics["eur_ret"] = float(returns['EURUSD=X'])
            risk_metrics["cny_ret"] = float(returns['CNY=X'])
        else:
            logger.warning("Yahoo Finance retornou dados insuficientes para cálculo de retorno.")

    except Exception as e:
        logger.error(f"Falha ao obter dados financeiros: {str(e)}", exc_info=True)

    # 2. LOCAIS ESTRATÉGICOS (Gargalos de Preço e Insumo)
    locations = {
        "China_Dalian": {"lat": 38.91, "lon": 121.60, "type": "Buyer", "desc": "China (Porto Dalian)"},
        "Panama_Canal": {"lat": 9.08, "lon": -79.68, "type": "Logistics_Global", "desc": "Canal do Panamá"},
        "Suez_Canal": {"lat": 30.58, "lon": 32.27, "type": "Inputs", "desc": "Canal de Suez (Insumos)"}
    }
    
    climate_rows = []

    for name, coords in locations.items():
        try:
            url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&daily=precipitation_sum,wind_speed_10m_max&timezone=UTC"
            
            # Timeout é crucial para não travar a pipeline
            res = requests.get(url, timeout=5)
            res.raise_for_status()
            
            data = res.json()
            daily = data.get('daily', {})
            
            # Extração segura dos valores (primeiro dia da previsão)
            rain = daily.get('precipitation_sum', [0])[0] if daily.get('precipitation_sum') else 0
            wind = daily.get('wind_speed_10m_max', [0])[0] if daily.get('wind_speed_10m_max') else 0
            
            status = "🟢 OPERAÇÃO NORMAL"
            
            # REGRAS DE RISCO NEGOCIAL
            if "China" in name and wind > 60: 
                status = "🔴 PORTO FECHADO (VENTO)"
            elif "Panama" in name and month <= 5 and rain < 5: 
                status = "🟡 NÍVEL BAIXO (FRETE CARO)"
            elif "Suez" in name and wind > 60: 
                status = "🟡 TEMPESTADE AREIA (ATRASO)"
            
            climate_rows.append({
                "Location": name, 
                "Risk_Status": status, 
                "Category": "Global",
                "Description": coords['desc']
            })
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro de conexão/API Clima para {name}: {e}")
            climate_rows.append({
                "Location": name, 
                "Risk_Status": "⚪ DADOS INDISPONÍVEIS (API ERROR)", 
                "Category": "Global",
                "Description": coords['desc']
            })
        except Exception as e:
            logger.error(f"Erro inesperado ao processar {name}: {e}", exc_info=True)

    return {"risk_metrics": risk_metrics, "alerts": []}, pd.DataFrame(climate_rows)