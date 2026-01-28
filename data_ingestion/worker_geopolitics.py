import asyncio
import os
import sys
from core.scout import NewsScout
from core.logger import get_logger

logger = get_logger("WorkerGeopolitics")

async def run_ingestion():
    logger.info("🌍 Iniciando Worker de Geopolítica...")
    
    # Verifica se tem chave da Hugging Face
    if not os.getenv("HUGGINGFACE_API_KEY"):
        logger.warning("⚠️ HUGGINGFACE_API_KEY não encontrada no .env!")
        logger.warning("   O Scout vai rodar, mas a classificação será 'NEUTRO'.")
        logger.warning("   Crie uma chave grátis em: https://huggingface.co/settings/tokens")
    
    scout = NewsScout(use_service_role=True)
    await scout.fetch_and_store()
    
    logger.info("✅ Worker finalizado.")

if __name__ == "__main__":
    asyncio.run(run_ingestion())