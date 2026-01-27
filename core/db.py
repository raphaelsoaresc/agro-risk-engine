# ARQUIVO: core/db.py
import os
import logging
import pytz
import time
from datetime import datetime  # Adicionado para suportar o novo método
from dotenv import load_dotenv
from supabase import create_client, Client

# Configuração de Log
logger = logging.getLogger(__name__)
load_dotenv()

# --- CACHE GLOBAL DE CONEXÕES ---
# Isso garante que o Python só abra UMA conexão por tipo, resolvendo o "Device busy"
_CLIENT_CACHE = {
    "service": None,
    "anon": None
}

class DatabaseManager:
    def __init__(self, use_service_role: bool = False):
        self.tz = pytz.timezone('America/Sao_Paulo')
        self.client = self._get_connection(use_service_role)

    def _get_connection(self, use_service_role: bool) -> Client:
        """
        Recupera a conexão do cache global ou cria uma nova.
        """
        key_type = "service" if use_service_role else "anon"
        
        # 1. Se já existe no cache, usa ela (Zero custo de conexão)
        if _CLIENT_CACHE[key_type] is not None:
            return _CLIENT_CACHE[key_type]

        # 2. Configuração de Credenciais
        url = os.getenv("SUPABASE_URL")
        if use_service_role:
            key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            mode_label = "ALTO PRIVILÉGIO (Service Role)"
        else:
            key = os.getenv("SUPABASE_KEY")
            mode_label = "PRIVILÉGIO LIMITADO (Anon)"

        if not url or not key:
            logger.critical(f"❌ Credenciais ausentes para: {mode_label}")
            return None

        # 3. Tentativa de Conexão com Retry Simples
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    logger.info(f"🔌 Reconectando Supabase ({mode_label})... Tentativa {attempt+1}")
                
                # Usamos o padrão da biblioteca, que é mais seguro
                client = create_client(url, key)
                
                # Salva no cache global
                _CLIENT_CACHE[key_type] = client
                logger.info(f"✅ Conexão estabelecida e cacheada: {mode_label}")
                return client

            except Exception as e:
                logger.warning(f"⚠️ Falha na conexão (Tentativa {attempt+1}): {e}")
                time.sleep(2) # Espera um pouco para o sistema liberar recursos
        
        logger.critical(f"❌ Falha fatal ao conectar Supabase após retries.")
        return None

    # --- MÉTODOS DE NEGÓCIO ---

    def get_active_subscribers(self):
        if not self.client: return []
        try:
            return self.client.table('subscribers').select('email, name').eq('is_active', True).execute().data
        except Exception as e:
            logger.error(f"Erro DB (Subscribers): {e}")
            return []

    def should_send_email(self, subscriber_id, region, risk_data):
        # Lógica de verificação de envio (placeholder ou real conforme necessidade)
        return True, "hash_placeholder"

    def log_email_sent(self, subscriber_id, region, risk_hash):
        pass 

    def save_risk_history(self, records):
        if not self.client or not records: return
        try:
            self.client.table("risk_history").insert(records).execute()
            logger.info(f"💾 Histórico salvo: {len(records)} registros.")
        except Exception as e:
            logger.error(f"Erro DB (Save Risk): {e}")

    def save_market_metrics(self, metrics):
        """
        Transforma métricas específicas em formato padronizado para o banco.
        De: {'basis_risk': 50, ...}
        Para: [{'category': 'BASIS', 'value': 50, ...}, ...]
        """
        if not self.client or not metrics: return

        try:
            records = []
            # Usa o horário do objeto timezone definido no __init__
            now = datetime.now(self.tz).isoformat() 

            # 1. Mapeamento de BASIS
            if 'basis_risk' in metrics:
                records.append({
                    "category": "BASIS",
                    "value": float(metrics.get('basis_risk', 0)),
                    "status": metrics.get('basis_status', 'Normal'),
                    "created_at": now
                })

            # 2. Mapeamento de FAI (Fertilizantes)
            if 'fertilizer_risk' in metrics:
                # Se fertilizer_risk for string/status, ajusta
                val = metrics.get('fertilizer_risk')
                status_text = metrics.get('fai_status', str(val))
                records.append({
                    "category": "FAI",
                    "value": 0.0, # FAI geralmente é qualitativo, valor 0 placeholder
                    "status": status_text,
                    "created_at": now
                })

            # 3. Mapeamento de CHINA
            if 'china_demand' in metrics:
                c_data = metrics['china_demand']
                # Se vier como dicionário {'score': 20, 'status': 'NORMAL'}
                if isinstance(c_data, dict):
                    records.append({
                        "category": "CHINA",
                        "value": float(c_data.get('score', 0)),
                        "status": c_data.get('status', 'Normal'),
                        "created_at": now
                    })
                else:
                    records.append({
                        "category": "CHINA",
                        "value": 0.0,
                        "status": str(c_data),
                        "created_at": now
                    })

            # Envia tudo de uma vez se houver registros
            if records:
                self.client.table("market_metrics").insert(records).execute()
                logger.info(f"📈 Métricas salvas: {len(records)} registros inseridos.")

        except Exception as e:
            logger.error(f"Erro DB (Metrics Transformation): {e}", exc_info=True)

    def get_already_sent_news_ids(self):
        if not self.client: return []
        try:
            res = self.client.table("sent_news_log").select("news_id").limit(100).execute()
            return [x['news_id'] for x in res.data] if res.data else []
        except Exception:
            return []

    def mark_news_as_sent(self, news_ids):
        pass