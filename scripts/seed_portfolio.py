# scripts/seed_portfolio.py
import uuid
import random
import time
import logging
from core.db import DatabaseManager

# Configuração de log simples
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def seed_database():
    """
    Popula o banco de dados com contratos fictícios para testes de estresse.
    Cria dois clusters claros: 
    1. MT (Alto Risco: Dívida alta, Score baixo, Logística ruim)
    2. PR (Baixo Risco: Dívida baixa, Score alto, Logística boa)
    """
    random.seed(42)
    db = DatabaseManager(use_service_role=True)
    tag = "DEV_TEST_DATASET"  
    
    logger.info(f"🚀 Iniciando Seed do Banco de Dados (TAG: {tag})...")
    
    try:
        # Limpeza de dados antigos para evitar duplicidade nos testes
        logger.info("🧹 Limpando dados antigos...")
        
        # Limpa resultados de backtest vinculados a essa tag
        sims = db.client.table("backtest_simulations").select("id").eq("simulation_name", tag).execute()
        for s in sims.data:
            db.client.table("backtest_results").delete().eq("simulation_id", s['id']).execute()
        
        # Limpa o portfólio
        db.client.table("credit_portfolio").delete().eq("simulation_tag", tag).execute()
        time.sleep(1) # Breve espera para consistência do DB
        
    except Exception as e:
        logger.warning(f"Aviso durante limpeza (pode ser a primeira execução): {e}")

    contracts = []
    
    # --- CLUSTER 1: MATO GROSSO (HIGH RISK PROFILE) ---
    logger.info("🌱 Gerando Cluster MT (Perfil de Alto Risco)...")
    mt_hubs = [
        {"name": "Sorriso", "lat": -12.54, "lon": -55.72},
        {"name": "Sinop", "lat": -11.86, "lon": -55.50},
        {"name": "Lucas", "lat": -13.07, "lon": -55.91}
    ]

    for i in range(50):
        hub = random.choice(mt_hubs)
        contracts.append({
            "id": str(uuid.uuid4()),
            "client_name": f"Produtor {hub['name']} {i+1:03d}",
            "latitude": hub['lat'] + random.uniform(-0.05, 0.05),
            "longitude": hub['lon'] + random.uniform(-0.05, 0.05),
            "state_code": "MT",
            "culture": "soja",
            "area_hectares": int(random.uniform(2000, 5000)),
            "estimated_yield_kg_ha": int(random.uniform(3200, 3500)),
            "loan_amount": round(random.uniform(10_000_000, 25_000_000), 2),
            "dist_to_port": int(random.uniform(2000, 2200)),
            # Perfil de Risco: Score Baixo + Alta Alavancagem
            "credit_score_serasa": random.randint(400, 600),
            "debt_to_income_ratio": random.uniform(0.75, 0.95),
            "payment_history_rating": "C",
            "simulation_tag": tag,
            "is_test_data": True
        })

    # --- CLUSTER 2: PARANÁ (LOW RISK PROFILE) ---
    logger.info("🌱 Gerando Cluster PR (Perfil de Baixo Risco)...")
    pr_hubs = [
        {"name": "Cascavel", "lat": -24.95, "lon": -53.45},
        {"name": "Toledo", "lat": -24.72, "lon": -53.74},
        {"name": "Londrina", "lat": -23.30, "lon": -51.16}
    ]

    for i in range(50):
        hub = random.choice(pr_hubs)
        contracts.append({
            "id": str(uuid.uuid4()),
            "client_name": f"Agro {hub['name']} {i+1:03d}",
            "latitude": hub['lat'] + random.uniform(-0.03, 0.03),
            "longitude": hub['lon'] + random.uniform(-0.03, 0.03),
            "state_code": "PR",
            "culture": "soja",
            "area_hectares": int(random.uniform(300, 800)),
            "estimated_yield_kg_ha": int(random.uniform(3800, 4200)),
            "loan_amount": round(random.uniform(1_000_000, 3_000_000), 2),
            "dist_to_port": int(random.uniform(400, 600)),
            # Perfil de Risco: Score Alto + Baixa Alavancagem
            "credit_score_serasa": random.randint(850, 980),
            "debt_to_income_ratio": random.uniform(0.10, 0.30),
            "payment_history_rating": "A",
            "simulation_tag": tag,
            "is_test_data": True
        })

    # Inserção em Batch
    logger.info(f"💾 Persistindo {len(contracts)} contratos no Supabase...")
    batch_size = 50
    for i in range(0, len(contracts), batch_size):
        batch = contracts[i:i + batch_size]
        db.client.table("credit_portfolio").insert(batch).execute()

    logger.info("✅ Seed concluído com sucesso.")

if __name__ == "__main__":
    seed_database()