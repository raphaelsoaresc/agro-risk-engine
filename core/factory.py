# core/factory.py
from core.strategies.mt_strategy import MatoGrossoStrategy
from core.strategies.pr_strategy import ParanaStrategy

class RegionalEngineFactory:
    @staticmethod
    def get_strategy(loc_data):
        state = loc_data.get('state_code', 'DEFAULT')
        if state == 'MT':
            return MatoGrossoStrategy()
        elif state == 'PR':
            return ParanaStrategy()
        # Fallback ou outras regiões futuras
        return ParanaStrategy() 

# core/pipeline.py (Refatorado)
class RiskPipeline:
    def __init__(self, mode, run_shadow_mode=True):
        self.mode = mode
        self.run_shadow_mode = run_shadow_mode
        self.legacy_engine = RiskEngine() # Motor monolítico antigo
        self.db = DatabaseManager(use_service_role=True)
        # ... (restante do init)

    def _process_regions(self):
        regions = self.config.get('locations', [])
        current_month = self.now_br.month

        for loc in regions:
            # 1. NOVO MOTOR (Strategy Pattern)
            strategy = RegionalEngineFactory.get_strategy(loc)
            
            new_scores = {
                "Logística": strategy.calculate_logistics_risk(self.df_market, loc),
                "Clima": strategy.calculate_climate_risk(self.df_climate, loc, current_month),
                "Mercado": strategy.calculate_market_risk(self.df_market)
            }
            
            # 2. SHADOW MODE (Comparação Silenciosa)
            if self.run_shadow_mode:
                legacy_scores, _ = self.legacy_engine.calculate_full_analysis(
                    self.df_market, loc['name'], self.df_climate, month=current_month
                )
                self._save_shadow_log(loc['name'], legacy_scores, new_scores)

            # 3. SEGUE O FLUXO USANDO O MOTOR NOVO
            # (Mantemos a compatibilidade de interface para o restante do pipeline)
            self.persister.save_region_risk(loc, sum(new_scores.values())/3, new_scores, {})

    def _save_shadow_log(self, loc_name, legacy, current):
        """Salva comparação no banco para análise do time de dados"""
        log_data = {
            "location": loc_name,
            "legacy_score": sum(legacy.values())/len(legacy),
            "new_strategy_score": sum(current.values())/len(current),
            "timestamp": datetime.now().isoformat()
        }
        # Inserir em uma tabela 'shadow_scoring_logs'
        self.db.client.table("shadow_scoring_logs").insert(log_data).execute()