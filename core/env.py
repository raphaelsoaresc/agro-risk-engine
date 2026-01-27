import os
import yaml
from dotenv import load_dotenv

# Carrega variáveis de ambiente (.env) logo no início
load_dotenv()

# ==============================================================================
# 1. GERENCIAMENTO DE ARQUIVOS YAML (Configurações do Projeto)
# ==============================================================================
def load_config():
    """
    Carrega a configuração unificada do projeto (settings.yaml).
    Suporta sobrescrita por dev.yaml para ambiente de desenvolvimento.
    """
    # Caminho base (sobe um nível da pasta core)
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_path, 'configs', 'settings.yaml')
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"❌ Arquivo de configuração não encontrado: {config_path}")

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Sobrescrita de Desenvolvimento (Opcional)
    dev_path = os.path.join(base_path, 'configs', 'dev.yaml')
    if os.path.exists(dev_path):
        # Opcional: print("🔧 [CONFIG] Carregando overrides de desenvolvimento...")
        with open(dev_path, 'r') as f:
            dev_config = yaml.safe_load(f)
            if dev_config:
                config.update(dev_config)

    return config

# ==============================================================================
# 2. GERENCIAMENTO DE E-MAIL (Sua Lógica Original Preservada)
# ==============================================================================
class EmailEnv:
    def __init__(self):
        # Tenta pegar a chave do Resend (Prioridade Alta)
        self.resend_key = os.getenv('RESEND_API_KEY')
        
        # Pega credenciais SMTP (Gmail/Outlook) para compatibilidade/backup
        self.sender = os.getenv('EMAIL_SENDER')
        self.password = os.getenv('EMAIL_PASSWORD')

def load_email_env():
    """
    Valida e retorna as credenciais de e-mail.
    Garante que temos pelo menos UMA forma de envio configurada.
    """
    env = EmailEnv()
    
    # Lógica híbrida:
    # 1. Se tiver Resend, ótimo.
    # 2. Se não tiver Resend, PRECISA ter Sender/Password (SMTP).
    
    if not env.resend_key:
        # Se não tem Resend, verifica se tem SMTP configurado
        if not env.sender or not env.password:
             # Só levanta erro se AMBOS estiverem faltando
             print("⚠️ AVISO: Nenhuma configuração de e-mail encontrada (Resend ou SMTP).")
             print("   Verifique se as secrets EMAIL_SENDER e EMAIL_PASSWORD estão no .env ou GitHub.")
             # Não vamos dar raise ValueError aqui para não travar o '--dry-run',
             # mas o envio falhará se tentado.
            
    return env