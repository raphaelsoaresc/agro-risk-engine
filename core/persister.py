from datetime import datetime
from core.db import DatabaseManager
from core.indicators.financial import calculate_fertilizer_affordability

class RiskPersister:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.now_iso = datetime.now(self.db.tz).isoformat()

    def save_region_risk(self, loc, final_score, raw_scores, metrics):
        self.db.client.table("risk_history").insert({
            "risk_name": loc['name'],
            "status": f"{final_score:.1f}", 
            "risk_level": "CRÍTICO" if final_score > 70 else ("ALERTA" if final_score > 40 else "NORMAL"),
            "region": loc.get('group', 'CREDIT'),
            "category": "LOGÍSTICA" if raw_scores['Logística'] > 50 else "MERCADO",
            "details": metrics, 
            "created_at": self.now_iso
        }).execute()

    def save_market_metrics(self, df_market, context):
        # FAI Calculation
        fai_status = calculate_fertilizer_affordability(
            df_market['ZS=F'], df_market['NG=F'], df_market['NTR']
        )
        
        self.db.save_market_metrics({
            "basis_risk": context.logistics_benchmark['value'],
            "basis_status": context.logistics_benchmark['status'],
            "china_demand": context.china_metrics,
            "fertilizer_risk": 1.0 if "ALERTA" in fai_status else 0.0, 
            "fai_status": fai_status 
        })

    def save_global_state(self, context, macro_corr):
        main_driver = max(context.pillar_sums, key=context.pillar_sums.get) if context.processed_count > 0 else "Clima"
        
        # Tendência 7d
        try:
            res_hist = self.db.client.table("risk_history").select("status").eq("risk_name", "GLOBAL_SCORE").order("created_at", desc=True).limit(7).execute()
            avg_7d = sum([float(r['status']) for r in res_hist.data]) / len(res_hist.data) if res_hist.data else 0
            trend_vs_7d = ((context.avg_global_score / avg_7d) - 1) if avg_7d > 0 else 0
        except Exception:
            trend_vs_7d = 0.0

        # Recomendação
        if context.critical_clusters:
            cluster_str = " | ".join(context.critical_clusters)
            rec = f"CRÍTICO: Anomalia Sistêmica em {cluster_str}. Ação imediata."
        elif context.avg_global_score > 70:
            rec = f"ALERTA GLOBAL: Risco sistêmico elevado em {main_driver}."
        else:
            rec = f"ESTÁVEL: Operação nominal. Monitorando {len(context.cluster_scores)} clusters."

        self.db.client.table("risk_history").insert({
            "risk_name": "GLOBAL_SCORE",
            "status": f"{context.avg_global_score:.1f}",
            "risk_level": "CRÍTICO" if context.critical_clusters or context.avg_global_score > 70 else ("ALERTA" if context.avg_global_score > 45 else "NORMAL"),
            "region": "GLOBAL",
            "category": "MARKET",
            "details": {
                "washout_risk": context.max_washout_risk,
                "logistics_benchmark": context.logistics_benchmark,
                "china_metrics": context.china_metrics,
                "main_driver": main_driver,
                "trend_7d": f"{trend_vs_7d:+.1%}",
                "macro_stress": f"{macro_corr:.2f}",
                "analyst_recommendation": rec,
                "critical_clusters": context.critical_clusters
            },
            "created_at": self.now_iso
        }).execute()

    def save_contract_risk(self, contract, pd_score, metrics):
        """
        Salva o histórico de risco com rastreabilidade total (Audit Trail).
        """
        try:
            payload = {
                "contract_id": contract['id'],
                "risk_name": contract['name'],
                "status": str(pd_score),
                "risk_level": self._get_risk_level(pd_score),
                "category": "CREDIT_PD",
                "details": metrics,
                "created_at": self.now_iso
            }
            
            res = self.db.client.table("risk_history").insert(payload).execute()
            
            # Verificação institucional de sucesso
            if hasattr(res, 'status_code') and res.status_code >= 400:
                logger.error(f"DB Error {res.status_code}: {res.data}")
                
        except Exception as e:
            # Log detalhado para debug rápido
            logger.error(f"⚠️ Falha na persistência: {str(e)}")
            # Em nível institucional, poderíamos implementar um fallback para arquivo local aqui

    def _get_risk_level(self, score):
        if score > 75: return "CRITICAL"
        if score > 50: return "HIGH"
        if score > 25: return "MODERATE"
        return "LOW"