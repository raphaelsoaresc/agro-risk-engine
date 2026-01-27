# core/backtest_engine.py
import pandas as pd
import numpy as np
import logging
from datetime import timedelta
from core.logger import get_logger
from core.climate_risk import ClimateIntelligence
from core.advisor import RiskAdvisor  # <--- IMPORT NOVO

logging.getLogger('InstitutionalBacktest').setLevel(logging.INFO)
logger = get_logger("InstitutionalBacktest")

class InstitutionalBacktestEngine:
    def __init__(self, risk_engine, db_manager):
        self.engine = risk_engine
        self.db = db_manager
        self.climate_intel = ClimateIntelligence()
        self.advisor = RiskAdvisor() # <--- INICIALIZAÇÃO DO NARRADOR

    def run_walk_forward(self, simulation_name, start_date, end_date, contracts):
        logger.info(f"🚀 Iniciando Backtest Institucional: {simulation_name}")
        
        warm_up_start = start_date - timedelta(days=45)
        full_market = self._load_historical_market(warm_up_start, end_date)
        if full_market is None or full_market.empty:
            raise ValueError("❌ Falha crítica: Dados de mercado insuficientes.")

        climate_map = self._load_historical_climate_map(contracts, start_date, end_date)

        # Gestão da Simulação (Limpeza e Criação)
        existing = self.db.client.table("backtest_simulations").select("id").eq("simulation_name", simulation_name).execute()
        if existing.data:
            sim_id = existing.data[0]['id']
            logger.info(f"🧹 Limpando dados antigos da simulação ID {sim_id}...")
            self.db.client.table("backtest_results").delete().eq("simulation_id", sim_id).execute()
            self.db.client.table("backtest_simulations").update({
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "status": "RUNNING"
            }).eq("id", sim_id).execute()
        else:
            res = self.db.client.table("backtest_simulations").insert({
                "simulation_name": simulation_name,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "status": "RUNNING"
            }).execute()
            sim_id = res.data[0]['id']

        dates = pd.date_range(start_date, end_date, freq='MS', tz='UTC')
        total_steps = len(dates)
        
        logger.info(f"⏳ Executando {total_steps} snapshots mensais...")

        for i, current_date in enumerate(dates):
            if i % max(1, total_steps // 10) == 0:
                logger.info(f"🔄 Progresso: {current_date.date()} ({i+1}/{total_steps})")
            
            pit_market = full_market.loc[:current_date]
            df_snapshot_climate = self._build_climate_snapshot(contracts, climate_map, current_date)

            snapshot_results = []

            for contract in contracts:
                try:
                    pd_score, metrics = self.engine.calculate_pd_metrics(
                        pit_market, 
                        contract['client_name'],
                        df_snapshot_climate, 
                        contract,
                        current_date.month
                    )

                    # --- CORREÇÃO XAI: GERAÇÃO DA NARRATIVA ---
                    # Aqui chamamos o Advisor explicitamente para este snapshot
                    narrative = self.advisor.generate_credit_narrative(pd_score, metrics)
                    # ------------------------------------------

                    lgd = 0.45 
                    expected_loss = (pd_score / 100.0) * lgd * contract['loan_amount']
                    
                    snapshot_results.append({
                        "simulation_id": sim_id,
                        "contract_id": contract['id'],
                        "sim_date": current_date.date().isoformat(),
                        "pd_score": float(pd_score),
                        "ltv_ratio": float(metrics.get('ltv', 0)),
                        "exposure_at_default": float(contract['loan_amount']),
                        "expected_loss": float(expected_loss),
                        "risk_justification": narrative # <--- SALVANDO O TEXTO GERADO
                    })

                except Exception as e:
                    logger.error(f"Erro no contrato {contract.get('client_name')}: {e}")
                    continue

            if snapshot_results:
                self._bulk_save(snapshot_results)

        self._calculate_final_metrics_sql(sim_id)
        logger.info(f"✅ Backtest {simulation_name} finalizado com sucesso.")

    # ... (Mantenha os métodos auxiliares _build_climate_snapshot, _calculate_final_metrics_sql, etc. iguais ao anterior)
    def _build_climate_snapshot(self, contracts, climate_map, current_date):
        snapshot_records = []
        lookback_date = (current_date - timedelta(days=7)).strftime('%Y-%m-%d')
        target_date_str = current_date.strftime('%Y-%m-%d')

        for contract in contracts:
            coords = (contract['latitude'], contract['longitude'])
            weather_df = climate_map.get(coords)
            
            if weather_df is not None:
                window = weather_df[
                    (weather_df['date'] <= target_date_str) & 
                    (weather_df['date'] > lookback_date)
                ]
                rain_7d = window['precipitation'].sum() if not window.empty else 0
                temp_max = window['temp_max'].mean() if not window.empty else 25
                
                status, score = self.climate_intel.analyze_risk(
                    {'rain_7d': rain_7d, 'temp_max': temp_max},
                    'production',
                    'S', 
                    current_date.month
                )
                snapshot_records.append({
                    'Location': contract['client_name'],
                    'Risk_Status': status,
                    'Risk_Score': score,
                    'Rain_7d': rain_7d,
                    'Temp_Max': temp_max
                })
        return pd.DataFrame(snapshot_records)

    def _calculate_final_metrics_sql(self, sim_id):
        try:
            all_results = []
            offset = 0
            while True:
                res = self.db.client.table("backtest_results").select("sim_date, expected_loss").eq("simulation_id", sim_id).range(offset, offset + 999).execute()
                if not res.data: break
                all_results.extend(res.data)
                offset += 1000
            
            df_agg = pd.DataFrame(all_results)
            if df_agg.empty: return

            portfolio_monthly_loss = df_agg.groupby('sim_date')['expected_loss'].sum()
            avg_monthly_loss = portfolio_monthly_loss.mean()
            total_cycle_loss = portfolio_monthly_loss.sum()
            portfolio_var_95 = np.percentile(portfolio_monthly_loss, 95)

            self.db.client.table("backtest_simulations").update({
                "total_expected_loss": float(total_cycle_loss), 
                "avg_log_loss": float(avg_monthly_loss),        
                "max_var_95": float(portfolio_var_95), 
                "status": "COMPLETED"
            }).eq("id", sim_id).execute()
        except Exception as e:
            logger.error(f"Erro na agregação: {e}")

    def _load_historical_market(self, start, end):
        start_str = start.strftime('%Y-%m-%d')
        end_str = end.strftime('%Y-%m-%d')
        res = self.db.client.table("market_prices").select("ticker, close, date").gte("date", start_str).lte("date", end_str).execute()
        if not res.data: return None
        df = pd.DataFrame(res.data)
        df['date'] = pd.to_datetime(df['date'], utc=True)
        df_pivot = df.pivot(index='date', columns='ticker', values='close').sort_index().ffill()
        for t in ["ZS=F", "USDBRL=X", "CL=F"]:
            if t not in df_pivot.columns: df_pivot[t] = np.nan
        return df_pivot

    def _load_historical_climate_map(self, contracts, start, end):
        climate_map = {}
        try:
            res = self.db.client.table("climate_historical_cache").select("*").execute()
            active_coords = set((c['latitude'], c['longitude']) for c in contracts)
            for row in res.data:
                coords = (row['latitude'], row['longitude'])
                if coords in active_coords:
                    climate_map[coords] = pd.DataFrame(row['data_json'])
            return climate_map
        except Exception as e:
            logger.error(f"Erro mapa clima: {e}")
            return {}

    def _bulk_save(self, data):
        if not data: return
        try:
            self.db.client.table("backtest_results").insert(data).execute()
        except Exception:
            pass