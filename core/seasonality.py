class SeasonalityManager:
    def __init__(self):
        # MATRIZ DE SENSIBILIDADE FENOLÓGICA POR ESTADO (UF)
        # Pesos calibrados conforme o calendário médio de cada estado
        self.state_weights = {
            "MT": { # Mato Grosso: Plantio precoce
                9: 0.6, 10: 1.0, 11: 1.5, 12: 2.0, 1: 1.8, 2: 0.8
            },
            "PR": { # Paraná: Ciclo ligeiramente deslocado
                10: 0.6, 11: 1.2, 12: 1.8, 1: 2.0, 2: 1.5, 3: 0.8
            },
            "GO": { # Goiás: Similar ao MT, mas com janelas variadas
                10: 0.7, 11: 1.3, 12: 1.9, 1: 2.0, 2: 1.2, 3: 0.7
            },
            "RS": { # Rio Grande do Sul: Ciclo tardio, alto risco em Jan/Fev
                10: 0.5, 11: 0.8, 12: 1.5, 1: 2.0, 2: 2.0, 3: 1.5, 4: 0.8
            },
            "MS": { # Mato Grosso do Sul
                9: 0.5, 10: 1.0, 11: 1.4, 12: 1.9, 1: 2.0, 2: 1.0
            },
            "BA": { # Bahia (MATOPIBA): Ciclo mais tardio
                11: 0.6, 12: 1.0, 1: 1.5, 2: 2.0, 3: 1.8, 4: 1.0
            }
        }
        self.default_weight = 1.0

    def get_state_weight(self, month, state_code):
        """
        Retorna o peso de sensibilidade fenológica exato para o Estado (UF).
        """
        state_data = self.state_weights.get(state_code.upper())
        if state_data:
            return state_data.get(month, self.default_weight)
        return self.default_weight

    def get_weight(self, month, region_group):
        if region_group == 'GLOBAL': return 1.0
        if month in self.weights:
            return self.weights[month].get(region_group, self.default_weight)
        return self.default_weight

class RiskAnalyzer:
    def __init__(self, raw_scores):
        self.scores = raw_scores
        self.manager = SeasonalityManager()

    def calculate_weighted_risk(self, target_month):
        # Passo 1: Definir o peso sazonal
        weight = self.manager.get_weight(target_month, 'BR')
        
        # Passo 2: Separar os componentes
        s_clima = self.scores.get('Clima', 0)
        s_log = self.scores.get('Logística', 0)
        s_mkt = self.scores.get('Mercado', 0)
        s_cambio = self.scores.get('Câmbio', 0)
        
        # Passo 3: Cálculo Inteligente (NOVA FÓRMULA)
        # Sazonalidade afeta CLIMA e LOGÍSTICA. Mercado/Câmbio têm peso 1.0 fixo.
        weighted_clima = s_clima * weight
        weighted_log = s_log * weight 
        
        numerator = (weighted_clima + weighted_log + s_mkt + s_cambio)
        denominator = (weight + weight + 1.0 + 1.0) # Soma dos pesos
        
        if denominator == 0: denominator = 1
            
        final_score = numerator / denominator
        
        return {
            'score_total': min(final_score, 100),
            'seasonality_weight': weight
        }