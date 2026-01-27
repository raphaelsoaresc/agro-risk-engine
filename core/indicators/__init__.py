from .technical import TechnicalIndicators
from .financial import FinancialIndicators
from .macro import MacroIndicators

class AgroIndicators(TechnicalIndicators, FinancialIndicators, MacroIndicators):
    """
    Fachada Unificada (Facade Pattern).
    Agrega todas as estratégias (Técnica, Financeira, Macro) em uma única classe
    para facilitar o uso pelo Engine.
    """
    pass