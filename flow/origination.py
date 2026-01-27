import yfinance as yf
import pandas as pd
import requests
import feedparser
from datetime import datetime

def get_season_context(month):
    """Retorna o contexto agrícola do mês atual para logging."""
    if 1 <= month <= 3: return "COLHEITA (SOJA) & LOGÍSTICA"
    elif 4 <= month <= 5: return "DESENVOLVIMENTO (MILHO SAFRINHA)"
    elif 6 <= month <= 9: return "ESCOAMENTO & NAVEGAÇÃO (ARCO NORTE)"
    else: return "PLANTIO (SAFRA NOVA)"

def get_data():
    """
    FOCO: Originação Inteligente com Matriz Sazonal Completa.
    Monitora: Sorriso (Fazenda), Sinop (Rodovia), Santarém (Hidrovia).
    """
    month = datetime.now().month
    season_name = get_season_context(month)
    print(f"🚜 Coletando Originação... [Fase: {season_name}]")
    
# 1. FINANCEIRO (Diesel & Custos)
    risk_metrics = {"diesel_proxy": 0.0}
    try:
        # Petróleo Brent como proxy do Diesel
        brent = yf.download('BZ=F', period='5d', progress=False)['Close']
        if not brent.empty:
            # CORREÇÃO: Usamos .values[-1] para pegar o valor numérico direto
            risk_metrics["diesel_proxy"] = float(brent.pct_change().values[-1])
    except Exception: pass

    # 2. NOTÍCIAS (Query adaptável baseada na fase)
    alerts = []
    query_map = {
        1: "soja atraso colheita mato grosso chuva grao ardido",
        2: "soja atraso colheita br-163 atoleiro",
        3: "soja produtividade colheita final",
        9: "baixo nivel rio tapajos santarem barcaca",
        10: "atraso plantio soja falta chuva mato grosso"
    }
    # Pega a query do mês atual ou usa uma genérica
    query = query_map.get(month, "agronegocio logistica soja milho brasil")
    
    try:
        rss_url = f"https://news.google.com/rss/search?q={query.replace(' ', '+')}&hl=pt-BR&gl=BR&ceid=BR:pt-419"
        feed = feedparser.parse(rss_url)
        for entry in feed.entries[:3]: alerts.append(entry.title.upper())
    except Exception: pass

    # 3. CLIMA & LÓGICA DE RISCO (A MATRIZ DE DECISÃO)
    locations = {
        "Sorriso_MT": {"lat": -12.54, "lon": -55.72, "type": "Farm_Soy"},
        "Londrina_PR": {"lat": -23.30, "lon": -51.16, "type": "Farm_Soy"},
        "Sinop_MT_BR163": {"lat": -11.86, "lon": -55.50, "type": "Road"},
        "Santarem_PA": {"lat": -2.44, "lon": -54.70, "type": "River"}
    }
    
    climate_rows = []
    
    for name, coords in locations.items():
        try:
            # Pega Chuva (mm) e Temperatura Max (°C)
            url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&daily=precipitation_sum,temperature_2m_max&timezone=America%2FSao_Paulo"
            res = requests.get(url, timeout=5).json()
            daily = res.get('daily', {})
            rain = daily.get('precipitation_sum', [0])[0]
            temp = daily.get('temperature_2m_max', [0])[0]
            
            status = "🟢 OPERAÇÃO NORMAL"
            
            # --- MATRIZ DE DECISÃO SAZONAL ---
            
            # [Q1] JANEIRO A MARÇO: COLHEITA (Chuva = Ruim)
            if 1 <= month <= 3:
                if coords['type'] == "Farm_Soy":
                    if rain > 30.0: status = "🔴 PARADA TOTAL (RISCO GRÃO ARDIDO)"
                    elif rain > 10.0: status = "🟡 UMIDADE ALTA (COLHEITA LENTA)"
                elif coords['type'] == "Road":
                    if rain > 40.0: status = "🔴 BLOQUEIO BR-163 (LAMA)"
                    elif rain > 20.0: status = "🟡 TRÁFEGO LENTO"
                elif coords['type'] == "River":
                    if rain > 15.0: status = "🟡 CARREGAMENTO LENTO (CHUVA)"

            # [Q2] ABRIL A MAIO: SAFRINHA MILHO (Seca = Ruim)
            elif 4 <= month <= 5:
                if coords['type'] == "Farm_Soy": # Aqui já é Milho na terra
                    if rain < 2.0: status = "🔴 ESTRESSE HÍDRICO (QUEBRA MILHO)"
                    elif rain < 10.0: status = "🟡 ALERTA DE SECA"

            # [Q3] JUNHO A SETEMBRO: LOGÍSTICA FLUVIAL (Seca Rios = Ruim)
            elif 6 <= month <= 9:
                if coords['type'] == "River": # Foco total em Santarém
                    if rain < 5.0: status = "🔴 RIO BAIXO (BARCAÇAS PARADAS)"
                    elif rain < 15.0: status = "🟡 CALADO REDUZIDO"
                elif coords['type'] == "Road":
                    status = "🟢 ESTRADA SECA (ÓTIMO FLUXO)"

            # [Q4] OUTUBRO A DEZEMBRO: PLANTIO (Seca = Ruim)
            else: 
                if coords['type'] == "Farm_Soy":
                    if rain < 5.0: status = "🔴 PLANTIO PARADO (SOLO SECO)"
                    elif rain < 15.0: status = "🟡 ATRASO NO PLANTIO"
                    elif rain > 50.0: status = "🟡 EXCESSO DE CHUVA (LAVAGEM)" # Muita chuva lava a semente
                elif coords['type'] == "River":
                     status = "🟢 NÍVEL EM RECUPERAÇÃO"

            climate_rows.append({"Location": name, "Risk_Status": status, "Category": "Origination"})
            
        except Exception:
            climate_rows.append({"Location": name, "Risk_Status": "⚪ DADOS INDISPONÍVEIS", "Category": "Origination"})

    return {"risk_metrics": risk_metrics, "alerts": alerts}, pd.DataFrame(climate_rows)