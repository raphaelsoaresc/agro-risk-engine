import argparse
import sys
from core.pipeline import RiskPipeline
from core.logger import get_logger

# Configuração de Log
logger = get_logger("MainEntry")

def main():
    """
    Ponto de entrada da aplicação.
    Responsabilidade: Parsear argumentos e iniciar o Pipeline.
    """
    parser = argparse.ArgumentParser(description="Agro Risk Intelligence - Execution Engine")
    parser.add_argument(
        "--mode", 
        choices=["morning", "watch"], 
        required=True, 
        help="Modo de execução: 'morning' (Relatório Matinal) ou 'watch' (Monitoramento Contínuo)"
    )
    args = parser.parse_args()

    try:
        # Instancia e executa o pipeline
        pipeline = RiskPipeline(mode=args.mode)
        pipeline.run()
        
    except KeyboardInterrupt:
        logger.info("🛑 Execução interrompida pelo usuário.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"❌ [ERRO CRÍTICO] Falha não tratada no nível superior: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()