import pytest
from core.indicators.fundamental import FundamentalIndicators

def test_washout_probability_critical():
    """Testa se o risco dispara quando há quebra de safra e alta de preço."""
    # Cenário: Clima Crítico + Preço subindo (Incentivo ao default)
    result = FundamentalIndicators.calculate_washout_probability(
        climate_risk_level="CRÍTICO",
        price_trend="ALTA",
        price_change_30d=0.15 # 15% de alta
    )
    
    assert result['score'] >= 80
    assert "MÁXIMO" in result['status']

def test_washout_probability_safe():
    """Testa cenário seguro."""
    result = FundamentalIndicators.calculate_washout_probability(
        climate_risk_level="NORMAL",
        price_trend="BAIXA",
        price_change_30d=-0.05
    )
    
    assert result['score'] == 0

# Para rodar: PYTHONPATH=. uv run pytest