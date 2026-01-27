import pandas as pd

class FinancialIndicators:
    """
    Especialista em Margens, Paridade, Relação de Troca e Estrutura de Mercado.
    Foca na viabilidade econômica da cadeia 'Farm-to-Port'.
    """

    @staticmethod
    def theoretical_parity(cbot_usd_bu: float, usd_brl: float) -> float:
        """
        Preço teórico da saca em BRL (Chicago * Dólar * Fator Conversão).
        Nota: Futuramente adicionaremos o Prêmio de Porto aqui.
        """
        return cbot_usd_bu * usd_brl * 2.2046

    @staticmethod
    def calculate_soy_crush_margin(soy_price, meal_price, oil_price):
        """
        [FASE 2] Calcula a Margem de Esmagamento Teórica (Crush Margin).
        Standard Yield: 1 bushel de soja (~60lb) produz ~44lb de farelo e ~11lb de óleo.
        """
        try:
            # 1. Contribuição do Farelo: preço em USD/ton curta (2000 lbs) -> converter para 44lb
            meal_contribution = (meal_price / 2000) * 44
            
            # 2. Contribuição do Óleo: preço em centavos/lb -> converter para 11lb e transformar em USD
            oil_contribution = (oil_price / 100) * 11
            
            # 3. Valor Bruto dos Subprodutos (Gross Processing Value)
            gross_value = meal_contribution + oil_contribution
            
            # 4. Margem Final: Valor Bruto - Custo da Soja (USD/bushel)
            crush_margin = gross_value - soy_price
            
            # Status de Atratividade para a Indústria Processadora
            if crush_margin > 2.5: 
                status = "EXCELENTE (Indústria Compradora)"
            elif crush_margin > 1.0: 
                status = "POSITIVA"
            else: 
                status = "CRÍTICA (Risco de Parada de Fábrica)"
            
            return {
                "value": round(crush_margin, 2),
                "status": status
            }
        except Exception:
            return {"value": 0, "status": "ERRO NO CÁLCULO"}

    @staticmethod
    def calculate_market_structure(current_contract_price, future_contract_price):
        """
        [FASE 2 - MAESTRIA] Detecta se o mercado está em Carry (Normal) ou Inverse (Risco).
        A estrutura de tela indica se vale a pena armazenar o grão ou vender imediatamente.
        """
        try:
            spread = future_contract_price - current_contract_price
            
            # Se o preço futuro é menor que o atual, o mercado está "Invertido"
            # Isso sinaliza escassez aguda e risco de o produtor ser forçado a vender na colheita.
            if spread < 0:
                return {
                    "status": "INVERSE (Risco de Armazenagem)",
                    "spread": round(spread, 2),
                    "risk_weight": 30 # Penalidade sugerida para o score
                }
            
            return {
                "status": "CARRY (Estrutura Normal)",
                "spread": round(spread, 2),
                "risk_weight": 0
            }
        except Exception:
            return {"status": "NEUTRO", "spread": 0, "risk_weight": 0}

    @staticmethod
    def calculate_terms_of_trade(revenue_series: pd.Series, cost_series: pd.Series) -> dict:
        """
        Efeito Tesoura (Terms of Trade).
        Receita (Soja) vs Custo (Insumo/Petróleo).
        """
        # Alinha as datas para garantir cálculo correto
        df = pd.concat([revenue_series, cost_series], axis=1).dropna()
        if df.empty: return {"ratio_rsi": 50, "trend": "NEUTRO"}
        
        rev = df.iloc[:, 0]
        cost = df.iloc[:, 1]
        ratio = cost / rev # Quanto maior, pior (custo pesa mais que receita)
        
        # RSI do Ratio (Velocidade da mudança de custo)
        delta = ratio.diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss.replace(0, 0.001)
        ratio_rsi = 100 - (100 / (1 + rs)).iloc[-1]
        
        return {
            "current_ratio": ratio.iloc[-1],
            "ratio_rsi": ratio_rsi,
            "trend": "PIORANDO" if ratio.iloc[-1] > ratio.mean() else "MELHORANDO"
        }
    
    # No core/indicators/financial.py
@staticmethod
def calculate_fertilizer_affordability(soy_series, gas_series, stock_series):
    """
    [REFINAMENTO QUANT] Implementa Lookback de 180 dias (6 meses) para Insumos.
    Simula o custo de aquisição real vs preço de Análise atual.
    """
    # Preço de Análise atual (Soja hoje)
    soy_current = soy_series.iloc[-1]
    
    # Preço de custo histórico (Insumos 6 meses atrás)
    # Assumindo que o DataFrame tem dados diários, 180 dias = ~6 meses
    if len(gas_series) > 180:
        gas_cost = gas_series.shift(180).iloc[-1]
        stock_cost = stock_series.shift(180).iloc[-1]
    else:
        # Fallback para o início da série se não houver 180 dias
        gas_cost = gas_series.iloc[0]
        stock_cost = stock_series.iloc[0]

    # Cálculo do Índice de Acessibilidade (Affordability)
    # Se a soja subiu mais que o custo travado do adubo = Margem Positiva
    affordability_ratio = soy_current / ((gas_cost + stock_cost) / 2)
    
    if affordability_ratio < 0.8: return "ALERTA: Margem de Insumos Crítica (Custo Travado Alto)"
    if affordability_ratio > 1.2: return "OPORTUNIDADE: Barter Favorável (Custo Travado Baixo)"
    return "ESTÁVEL"