from collections import defaultdict
from core.seasonality import RiskAnalyzer

class RiskContext:
    """
    Responsável por manter o estado da execução atual (In-Memory).
    Acumula scores, detecta clusters e mantém trackers de benchmarks.
    """
    def __init__(self):
        self.cluster_scores = defaultdict(list)
        self.cluster_details = defaultdict(list)
        self.critical_clusters = []
        self.pillar_sums = {"Mercado": 0, "Logística": 0, "Clima": 0, "Câmbio": 0}
        self.processed_count = 0
        self.avg_global_score = 0.0
        
        # Trackers Específicos
        self.max_washout_risk = {"score": 0, "status": "NORMAL", "location": "Global", "moisture": "N/A"}
        self.logistics_benchmark = {"value": 0, "status": "Normal", "port": "Santos", "display_val": "0.00"}
        self.china_metrics = {"status": "Normal", "margin": "Estável"}

        # Métricas de Portfólio
        self.total_exposure_brl = 0.0
        self.weighted_pd_sum = 0.0
        self.exposure_at_critical_risk = 0.0 # VaR (Value at Risk)
        self.contract_count = 0

    def update_metrics(self, loc_name, raw_scores, metrics, climate_context):
        """Atualiza os acumuladores globais e trackers específicos."""
        # 1. Acumuladores de Pilares
        for pilar in self.pillar_sums:
            self.pillar_sums[pilar] += raw_scores.get(pilar, 0)
        self.processed_count += 1

        # 2. Tracker Washout
        current_w_score = metrics.get('washout_risk', {}).get('score', 0)
        if current_w_score > self.max_washout_risk['score']:
            self.max_washout_risk = {
                "score": current_w_score,
                "status": metrics['washout_risk']['status'],
                "location": loc_name.replace('_', ' '),
                "moisture": climate_context['status_desc']
            }

        # 3. Tracker Santos (Benchmark)
        if loc_name == 'Porto_Santos':
            val_basis = raw_scores['Logística']
            self.logistics_benchmark = {
                "value": val_basis,
                "status": metrics.get('basis_status', 'Normal').split(':')[-1].strip(),
                "port": "Santos",
                "display_val": f"{val_basis:.2f}" if val_basis != 0 else "Paridade"
            }

        # 4. Tracker China
        if 'China' in loc_name:
            # Nota: O valor exato da variação vem de fora, mas o status vem das métricas
            self.china_metrics["status"] = metrics.get('china_demand', {}).get('status', 'Normal')

    def register_score(self, cluster_name, loc_name, raw_scores, current_month):
        """Calcula score ponderado e registra no cluster."""
        analyzer = RiskAnalyzer(raw_scores)
        weighted = analyzer.calculate_weighted_risk(target_month=current_month)
        final_score = weighted['score_total']

        self.cluster_scores[cluster_name].append(final_score)
        if final_score > 75:
            self.cluster_details[cluster_name].append(loc_name)
            
        return final_score

    def analyze_systemic_risk(self):
        """Processa os clusters para definir se há crise sistêmica."""
        for cluster_name, scores in self.cluster_scores.items():
            avg_cluster_score = sum(scores) / len(scores)
            critical_members = len(self.cluster_details[cluster_name])
            total_members = len(scores)
            
            is_critical = False
            reason = ""

            if cluster_name == 'GLOBAL_CHOKEPOINTS':
                if critical_members >= 1:
                    is_critical = True
                    reason = f"Gargalo Logístico em {self.cluster_details[cluster_name][0]}"
            else:
                if avg_cluster_score > 70:
                    is_critical = True
                    reason = f"Colapso Regional (Média: {avg_cluster_score:.0f})"
                elif critical_members >= (total_members / 2):
                    is_critical = True
                    reason = f"Falha Sistêmica ({critical_members}/{total_members} locais)"

            if is_critical:
                self.critical_clusters.append(f"{cluster_name}: {reason}")

        # Score Global
        all_scores = [s for scores in self.cluster_scores.values() for s in scores]
        self.avg_global_score = sum(all_scores) / len(all_scores) if all_scores else 0

    def update_portfolio_metrics(self, pd_score, loan_amount, collateral_status):
        """
        Acumula métricas para visão consolidada de carteira.
        """
        self.contract_count += 1
        self.total_exposure_brl += loan_amount
        self.weighted_pd_sum += (pd_score * loan_amount)
        
        if collateral_status in ['WARNING', 'CRITICAL_UNCOVERED'] or pd_score > 70:
            self.exposure_at_critical_risk += loan_amount

    def get_portfolio_summary(self):
        avg_pd = self.weighted_pd_sum / self.total_exposure_brl if self.total_exposure_brl > 0 else 0
        return {
            "total_exposure": round(self.total_exposure_brl, 2),
            "avg_weighted_pd": round(avg_pd, 2),
            "value_at_risk": round(self.exposure_at_critical_risk, 2),
            "risk_concentration_ratio": round(self.exposure_at_critical_risk / self.total_exposure_brl, 4) if self.total_exposure_brl > 0 else 0
        }