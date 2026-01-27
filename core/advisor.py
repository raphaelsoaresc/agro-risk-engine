# core/advisor.py
import logging

logger = logging.getLogger(__name__)

class RiskAdvisor:
    """
    XAI Engine (Explainable AI) para Risco de Crédito Agro.
    Gera narrativas determinísticas baseadas em gatilhos fundamentais.
    """

    def generate_credit_narrative(self, pd_score, metrics):
        """
        Constrói um laudo técnico detalhado explicando o 'Porquê' do score.
        Estrutura: [VEREDITO] + [CAUSA RAIZ CLIMÁTICA] + [SAÚDE FINANCEIRA] + [FATOR LOGÍSTICO].
        """
        narrative_parts = []

        # 1. VEREDITO INICIAL (O "Headline")
        if pd_score > 70:
            narrative_parts.append("🔴 PERFIL CRÍTICO: Probabilidade de Default elevada.")
        elif pd_score > 40:
            narrative_parts.append("🟡 PERFIL ALERTA: Sinais de deterioração da capacidade de pagamento.")
        else:
            narrative_parts.append("🟢 PERFIL ROBUSTO: Operação dentro dos parâmetros de segurança.")

        # 2. ANÁLISE CLIMÁTICA (A Causa Raiz Biológica)
        # Extrai a perda de produtividade calculada no engine
        yield_loss_str = metrics.get('yield_loss_est', '0%')
        yield_loss_val = float(yield_loss_str.strip('%'))
        
        if yield_loss_val > 15.0:
            narrative_parts.append(f"Quebra de safra severa estimada em {yield_loss_str} devido a estresse térmico/hídrico na janela crítica.")
        elif yield_loss_val > 5.0:
            narrative_parts.append(f"Perda marginal de produtividade ({yield_loss_str}) detectada, pressionando levemente o fluxo de caixa.")
        else:
            narrative_parts.append("Condições climáticas favoráveis sustentam a produtividade projetada.")

        # 3. ANÁLISE FINANCEIRA (LTV e Garantias)
        ltv = metrics.get('ltv', 0)
        collateral_val = metrics.get('collateral_value_brl', 0)
        
        if ltv > 1.0:
            narrative_parts.append(f"⚠️ ESTRUTURA DESCOBERTA: LTV projetado de {ltv:.2f}x indica insuficiência de garantias (Colateral: R$ {collateral_val:,.0f}).")
        elif ltv > 0.7:
            narrative_parts.append(f"Alavancagem moderada (LTV {ltv:.2f}x), exigindo monitoramento da liquidez.")
        else:
            narrative_parts.append(f"Excelente cobertura de garantias (LTV {ltv:.2f}x), mitigando risco de perda final (LGD).")

        # 4. ANÁLISE LOGÍSTICA (O "Custo Brasil")
        # Se o preço do frete/basis estiver estressado
        basis_status = metrics.get('basis_status', 'Normal')
        if "Estressado" in basis_status:
            narrative_parts.append("Logística pressionada: Custo de escoamento corrói a margem líquida do produtor.")
        
        # 5. ANÁLISE COMPORTAMENTAL (Serasa/Dívida)
        # Recuperamos isso indiretamente se o PD for alto mas o clima for bom
        if pd_score > 50 and yield_loss_val < 5.0:
            narrative_parts.append("Risco impulsionado majoritariamente por fatores comportamentais (Score de Crédito/Endividamento prévio).")

        # Montagem Final
        full_narrative = " ".join(narrative_parts)
        return full_narrative