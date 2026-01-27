import yfinance as yf
import pandas as pd
import requests
import feedparser
from datetime import datetime

def get_port_season_context(month):
    """Define o nível de estresse logístico nos portos BR."""
    if 2 <= month <= 6: return "ALTA TEMPORADA (SOJA) - FILA MÁXIMA"
    elif 7 <= month <= 9: return "ALTA TEMPORADA (MILHO) - FILA INTENSA"
    else: return "ENTRESSAFRA (MANUTENÇÃO/BAIXA)"

def get_data():
    """
    FOCO: Exportação (Portos BR).
    Monitora: Santos (Sudeste), Paranaguá (Sul) e Itaqui (Arco Norte).
    Sensível ao Line-up (Fila de Navios) da época.
    """
    month = datetime.now().month
    season_context = get_port_season_context(month)
    print(f"🚢 Coletando Exportação... [Contexto: {season_context}]")
    
    # Define se é época crítica de embarque (Line-up cheio)
    is_peak_season = 2 <= month <= 9 

# 1. FINANCEIRO (Basis/Prêmio & Câmbio)
    risk_metrics = {"diff_basis": 0.0, "usd_ret": 0.0}
    try:
        tickers = ['BRL=X', 'ZS=F']
        data = yf.download(tickers, period='5d', progress=False)['Close']
        
        if not data.empty:
            # CORREÇÃO: Usamos .values[-1] aqui também
            risk_metrics["usd_ret"] = float(data['BRL=X'].pct_change().values[-1])
            
            # Para o Basis (Volatilidade)
            soy_vol = data['ZS=F'].pct_change(fill_method=None).std()
            risk_metrics["diff_basis"] = float(soy_vol * 2.0)
    except Exception: pass

    # 2. NOTÍCIAS (Focada em Line-up e Demurrage)
    alerts = []
    try:
        # Busca por filas, multas e problemas portuários
        feed = feedparser.parse("https://news.google.com/rss/search?q=porto+santos+paranagua+fila+navios+demurrage+arco+norte&hl=pt-BR&gl=BR&ceid=BR:pt-419")
        for entry in feed.entries[:3]: alerts.append(entry.title.upper())
    except Exception: pass

    # 3. CLIMA PORTUÁRIO
    # Monitoramos os 3 principais exaustores do Brasil
    ports = {
        "Santos_SP": {"lat": -23.96, "lon": -46.33, "type": "Main_Hub"},
        "Paranagua_PR": {"lat": -25.51, "lon": -48.50, "type": "Main_Hub"},
        "Itaqui_MA": {"lat": -2.57, "lon": -44.36, "type": "North_Arc"} # Saída do Arco Norte
    }
    
    climate_rows = []
    for name, coords in ports.items():
        try:
            url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&daily=precipitation_sum,wind_speed_10m_max&timezone=America%2FSao_Paulo"
            res = requests.get(url, timeout=5).json()
            daily = res.get('daily', {})
            rain = daily.get('precipitation_sum', [0])[0]
            wind = daily.get('wind_speed_10m_max', [0])[0]
            
            status = "🟢 OPERAÇÃO NORMAL"
            
            # --- LÓGICA DE PORTO (CHUVA = HATCH CLOSED) ---
            
            # Regra de Chuva (Paralisa embarque)
            if rain > 10.0:
                if is_peak_season:
                    # Na alta temporada, chuva gera multas (Demurrage) pesadas
                    status = "🔴 CHUVA/LINE-UP (DEMURRAGE RISK)"
                else:
                    status = "🟡 ATRASO OPERACIONAL (CHUVA)"
            
            # Regra de Vento (Gruas param)
            if wind > 45.0:
                status = "🔴 VENDAVAL (GRUAS PARADAS)"

            climate_rows.append({"Location": name, "Risk_Status": status, "Category": "Export"})
        except Exception:
            climate_rows.append({"Location": name, "Risk_Status": "⚪ DADOS INDISPONÍVEIS", "Category": "Export"})

    return {"risk_metrics": risk_metrics, "alerts": alerts}, pd.DataFrame(climate_rows)