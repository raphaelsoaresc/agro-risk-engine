# ARQUIVO: core/scout.py
import feedparser
import hashlib
import logging
import os
import httpx
import traceback
from datetime import datetime, timedelta
from core.db import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("NewsScout")

class NewsScout:
    """
    OSINT v4.8 - Otimizado para evitar vazamento de conexões.
    """
    
    def __init__(self, use_service_role=False):
        self.db = DatabaseManager(use_service_role=use_service_role)
        self.hf_token = os.getenv("HUGGINGFACE_API_KEY")
        self.hf_api_url = "https://router.huggingface.co/hf-inference/models/facebook/bart-large-mnli"
        
        self.feeds = {
            'logistica_br': 'https://news.google.com/rss/search?q=greve+caminhoneiros+OR+bloqueio+br-163+OR+porto+santos+paralisacao&hl=pt-BR&gl=BR&ceid=BR:pt-419',
            'geopolitica': 'https://news.google.com/rss/search?q=suez+canal+blocked+OR+panama+canal+drought+OR+war+trade+routes&hl=en-US&gl=US&ceid=US:en',
            'mercado': 'https://news.google.com/rss/search?q=soja+recorde+safra+OR+milho+exportacao+brasil&hl=pt-BR&gl=BR&ceid=BR:pt-419'
        }

    def _generate_id(self, link):
        return hashlib.md5(link.encode('utf-8')).hexdigest()

    async def _analyze_with_ai(self, text, client: httpx.AsyncClient):
        """Analisa o texto usando um cliente HTTP já existente."""
        if not self.hf_token:
            return "NEUTRO", 0.0

        headers = {"Authorization": f"Bearer {self.hf_token}"}
        payload = {
            "inputs": text,
            "parameters": {"candidate_labels": ["Supply Chain Crisis", "Market Opportunity", "Irrelevant News", "Weather Disaster"]}
        }

        try:
            # REUTILIZA o cliente passado por parâmetro
            response = await client.post(self.hf_api_url, headers=headers, json=payload, timeout=20.0)
            
            if response.status_code != 200:
                logger.warning(f"⚠️ HF API Error: {response.status_code}")
                return "NEUTRO", 0.0

            result = response.json()
            data = result[0] if isinstance(result, list) else result
            
            if 'labels' in data and 'scores' in data:
                return data['labels'][0], data['scores'][0]
            return "NEUTRO", 0.0

        except Exception:
            logger.error(f"❌ Erro na análise de IA: {traceback.format_exc()}")
            return "NEUTRO", 0.0

    async def fetch_and_store(self):
        """Varredura principal com gerenciamento eficiente de conexões."""
        logger.info("🕵️ Scout AI: Iniciando varredura inteligente...")
        
        # O 'async with' fora do loop garante que apenas UM cliente seja usado para tudo
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as http_client:
            for category, url in self.feeds.items():
                try:
                    response = await http_client.get(url)
                    feed = feedparser.parse(response.text)
                    
                    for entry in feed.entries[:5]:
                        alert_id = self._generate_id(entry.link)
                        
                        # Passamos o http_client para a função de IA
                        label, score = await self._analyze_with_ai(entry.title, http_client)
                        
                        risk_level = "NEUTRO"
                        if score > 0.5:
                            if label in ["Supply Chain Crisis", "Weather Disaster"]:
                                risk_level = "CRÍTICO"
                            elif label == "Market Opportunity":
                                risk_level = "OPORTUNIDADE"
                        
                        if risk_level != "NEUTRO":
                            # Usa o fuso horário do DatabaseManager (Brasília)
                            now = datetime.now(self.db.tz)
                            payload = {
                                "id": alert_id,
                                "title": entry.title,
                                "category": category,
                                "risk_level": risk_level,
                                "source_url": entry.link,
                                "expires_at": (now + timedelta(hours=24)).isoformat(),
                                "created_at": now.isoformat()
                            }
                            
                            try:
                                self.db.client.table('scout_cache').upsert(payload).execute()
                                logger.info(f"🧠 AI ({risk_level}): {entry.title[:40]}...")
                            except Exception as e:
                                logger.error(f"Erro ao salvar no banco: {e}")

                except Exception as e:
                    logger.error(f"Erro ao processar feed {category}: {e}")
                    continue

    def get_alerts(self, filter_sent=True):
        """Recupera alertas válidos do banco."""
        if not self.db.client: return []
        try:
            now = datetime.now(self.db.tz).isoformat()
            sent_ids = self.db.get_already_sent_news_ids() if filter_sent else []

            res = self.db.client.table('scout_cache')\
                .select("*")\
                .gt('expires_at', now)\
                .order('created_at', desc=True)\
                .execute()
            
            all_alerts = res.data if res.data else []

            if filter_sent and sent_ids:
                return [a for a in all_alerts if a['id'] not in sent_ids]
            
            return all_alerts

        except Exception as e:
            logger.error(f"Erro ao recuperar alertas: {e}")
            return []