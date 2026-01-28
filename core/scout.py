import feedparser
import logging
import os
import httpx
from datetime import datetime
from core.db import DatabaseManager

logger = logging.getLogger("NewsScout")

class NewsScout:
    """
    Agente de Inteligência Geopolítica.
    Lê RSS Feeds -> Classifica com IA (Zero-Shot) -> Salva Alertas Críticos.
    """
    
    def __init__(self, use_service_role=True):
        self.db = DatabaseManager(use_service_role=use_service_role)
        self.hf_token = os.getenv("HUGGINGFACE_API_KEY")
        # Modelo BART Large (Ótimo para classificação de texto sem treino)
        self.hf_api_url = "https://api-inference.huggingface.co/models/facebook/bart-large-mnli"
        
        # Feeds focados em Risco Sistêmico
        self.feeds = {
            'LOGISTICA_GLOBAL': 'https://news.google.com/rss/search?q=suez+canal+blocked+OR+panama+canal+drought+OR+red+sea+attacks&hl=en-US&gl=US&ceid=US:en',
            'GUERRA_SANCOES': 'https://news.google.com/rss/search?q=war+ukraine+grain+deal+OR+trade+sanctions+china+usa+soybean&hl=en-US&gl=US&ceid=US:en',
            'CLIMA_EXTREMO': 'https://news.google.com/rss/search?q=el+nino+impact+crops+brazil+drought+argentina+soybean&hl=en-US&gl=US&ceid=US:en',
            'GREVES_BR': 'https://news.google.com/rss/search?q=greve+caminhoneiros+brasil+OR+paralisacao+porto+santos+paranagua&hl=pt-BR&gl=BR&ceid=BR:pt-419'
        }

    async def _analyze_with_ai(self, text, client):
        """
        Usa a Hugging Face para decidir se a notícia é perigosa.
        """
        if not self.hf_token:
            # Se não tiver token, retorna neutro para não quebrar
            return "NEUTRO", 0.0

        headers = {"Authorization": f"Bearer {self.hf_token}"}
        payload = {
            "inputs": text,
            "parameters": {
                "candidate_labels": ["Supply Chain Disruption", "War Conflict", "Market Opportunity", "Irrelevant"],
                "multi_label": False
            }
        }

        try:
            response = await client.post(self.hf_api_url, headers=headers, json=payload, timeout=10.0)
            
            if response.status_code != 200:
                return "NEUTRO", 0.0

            result = response.json()
            # A API retorna {labels: [...], scores: [...]}
            if isinstance(result, dict) and 'labels' in result and 'scores' in result:
                top_label = result['labels'][0]
                top_score = result['scores'][0]
                return top_label, top_score
            
            return "NEUTRO", 0.0

        except Exception as e:
            logger.error(f"Erro IA: {e}")
            return "NEUTRO", 0.0

    async def fetch_and_store(self):
        logger.info("🕵️ Scout AI: Iniciando varredura geopolítica...")
        
        alerts_to_save = []
        
        async with httpx.AsyncClient() as client:
            for category, url in self.feeds.items():
                try:
                    # O feedparser é síncrono, mas o request é rápido
                    feed = feedparser.parse(url)
                    
                    # Analisa apenas as 3 mais recentes para economizar API e focar no "Agora"
                    for entry in feed.entries[:3]:
                        label, score = await self._analyze_with_ai(entry.title, client)
                        
                        # Lógica de Risco
                        risk_level = "NEUTRO"
                        if score > 0.6: # Confiança mínima
                            if label in ["Supply Chain Disruption", "War Conflict"]:
                                risk_level = "CRÍTICO"
                            elif label == "Market Opportunity":
                                risk_level = "OPORTUNIDADE"
                        
                        # Só salvamos se for relevante
                        if risk_level in ["CRÍTICO", "ALERTA", "OPORTUNIDADE"]:
                            alerts_to_save.append({
                                "category": category,
                                "headline": entry.title,
                                "risk_level": risk_level,
                                "source_url": entry.link,
                                "is_active": True,
                                "created_at": datetime.utcnow().isoformat()
                            })
                            logger.info(f"🚨 DETECTADO [{risk_level}]: {entry.title[:50]}...")

                except Exception as e:
                    logger.error(f"Erro no feed {category}: {e}")

        # Salva no Banco
        if alerts_to_save:
            try:
                self.db.client.table('geopolitical_alerts').insert(alerts_to_save).execute()
                logger.info(f"💾 {len(alerts_to_save)} alertas geopolíticos salvos.")
            except Exception as e:
                logger.error(f"Erro ao salvar no DB: {e}")
        else:
            logger.info("✅ Nenhuma ameaça geopolítica relevante detectada agora.")