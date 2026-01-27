import pandas as pd
import numpy as np
from scipy.stats import pearsonr
from core.db import DatabaseManager
from core.logger import get_logger

logger = get_logger("ValidationEngine")

class IBGEValidationEngine:
    def __init__(self, db_manager):
        self.db = db_manager

    def run_accuracy_test(self, simulation_tag: str):
        """
        Confronta a previsão do Modelo (Backtest) com a Realidade (CONAB/IBGE).
        """
        logger.info(f"⚖️ Iniciando Auditoria de Acurácia | Tag: {simulation_tag}")

        # 1. Busca Resultados do Modelo (O que o robô previu)
        sim_id = self._get_sim_id(simulation_tag)
        if not sim_id:
            logger.error("Simulação não encontrada.")
            return {}

        # Pega PD Score e Estado do contrato
        # Precisamos fazer um JOIN com a tabela de portfolio para saber o estado
        res_model = self.db.client.table("backtest_results")\
            .select("contract_id, pd_score, credit_portfolio(state_code)")\
            .eq("simulation_id", sim_id)\
            .execute()
        
        if not res_model.data:
            logger.error("Sem resultados de backtest para analisar.")
            return {}

        # Normaliza dados do modelo
        data_model = []
        for row in res_model.data:
            state = row['credit_portfolio']['state_code']
            pd_score = row['pd_score']
            data_model.append({'state': state, 'predicted_risk': pd_score})
        
        df_model = pd.DataFrame(data_model)
        # Média de risco previsto por estado
        df_model_agg = df_model.groupby('state')['predicted_risk'].mean().reset_index()

        # 2. Busca a Verdade Terrestre (O que aconteceu na CONAB)
        res_truth = self.db.client.table("official_crop_stats")\
            .select("state_code, yield_kg_ha")\
            .eq("crop_year", "2023/2024")\
            .execute()
        
        df_truth = pd.DataFrame(res_truth.data)
        
        # 3. Cruzamento (Merge)
        df_final = pd.merge(df_model_agg, df_truth, left_on='state', right_on='state_code')
        
        if df_final.empty:
            logger.warning("Não foi possível cruzar estados do modelo com dados da CONAB.")
            return {}

        return self._calculate_metrics(df_final)

    def _calculate_metrics(self, df):
        """
        Calcula a correlação entre Risco Previsto (PD) e Quebra Real (Yield Inverso).
        Teoria: Quanto MENOR a produtividade (Yield), MAIOR deve ser o PD.
        Logo, a correlação deve ser NEGATIVA forte.
        """
        # Correlação de Pearson
        corr, p_value = pearsonr(df['predicted_risk'], df['yield_kg_ha'])
        
        # Erro Médio (apenas ilustrativo, pois as escalas são diferentes)
        # O importante aqui é a Direção da Correlação.
        
        metrics = {
            "correlation": corr, # Esperado: Negativo (ex: -0.80)
            "p_value": p_value,
            "sample_size": len(df),
            "details_by_state": df.to_dict(orient='records')
        }
        
        logger.info(f"📊 Resultado da Auditoria: Correlação {corr:.2f} (P-Value: {p_value:.4f})")
        return metrics

    def _get_sim_id(self, tag):
        res = self.db.client.table("backtest_simulations").select("id").eq("simulation_name", tag).execute()
        return res.data[0]['id'] if res.data else None