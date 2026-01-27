import os
import re

# --- CONFIGURAÇÃO ---
ROOT_DIR = "."  # Diretório atual
DRY_RUN = False  # Se True, apenas mostra o que faria. Se False, aplica as mudanças.

# Extensões para verificar
EXTENSIONS = {'.py', '.yaml', '.yml', '.md', '.txt'}

# Pastas para ignorar
IGNORE_DIRS = {'.git', '.venv', 'venv', '__pycache__', '.idea', '.vscode'}

# 1. Substituições Diretas (Case Insensitive)
# Formato: "Termo Antigo": "Termo Novo"
REPLACEMENTS = {
    r"\bMVP\b": "Projeto",
    r"\bHigh Ticket\b": "Enterprise Grade",
    r"\bSaaS\b": "System",
    r"\bVenda\b": "Análise",
    r"\bAssinatura\b": "Configuração",
    r"\bPlano Beta\b": "Modo de Teste",
    r"\bComprar\b": "Acessar",
    r"Agro Risk Engine - Data Pipeline": "Agro Risk Engine - Data Pipeline",
}

# 2. Termos Sensíveis (Apenas avisa para você checar manualmente)
WARNING_TERMS = ["Cliente", "Pagamento", "Preço", "Lucro"]

def sanitize_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception:
        return # Pula arquivos binários ou com encoding estranho

    original_content = content
    modified = False
    
    # Aplica substituições
    for pattern, replacement in REPLACEMENTS.items():
        # Regex com ignore case
        new_content = re.sub(pattern, replacement, content, flags=re.IGNORECASE)
        if new_content != content:
            print(f"   [MUDANÇA] '{pattern}' -> '{replacement}'")
            content = new_content
            modified = True

    # Remove Linhas de TODO com viés comercial
    lines = content.split('\n')
    new_lines = []
    for line in lines:
        if "# TODO" in line or "# FIXME" in line:
            if any(x in line.lower() for x in ["vender", "pagamento", "Configuração", "preço"]):
                print(f"   [REMOVIDO TODO] {line.strip()}")
                modified = True
                continue # Pula essa linha (deleta)
        new_lines.append(line)
    
    content = '\n'.join(new_lines)

    # Checa termos sensíveis (Cliente)
    for term in WARNING_TERMS:
        if term in content:
            print(f"   ⚠️  [ATENÇÃO] Termo '{term}' encontrado. Verifique o contexto manualmente.")

    # Salva se houve mudança e não é simulação
    if modified:
        if not DRY_RUN:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✅ Arquivo atualizado: {filepath}")
        else:
            print(f"🔍 [SIMULAÇÃO] Arquivo teria sido atualizado: {filepath}")

def main():
    print(f"🛡️  INICIANDO SANITIZAÇÃO DO PROJETO (Modo: {'SIMULAÇÃO' if DRY_RUN else 'GRAVAÇÃO'})")
    print("="*60)

    for root, dirs, files in os.walk(ROOT_DIR):
        # Filtra pastas ignoradas
        dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]

        for file in files:
            if any(file.endswith(ext) for ext in EXTENSIONS):
                # Pula o próprio script
                if file == "sanitize_project.py": continue
                
                filepath = os.path.join(root, file)
                print(f"\nVerificando: {filepath}...")
                sanitize_file(filepath)

    print("\n" + "="*60)
    if DRY_RUN:
        print("FIM DA SIMULAÇÃO. Para aplicar, mude DRY_RUN = False no script.")
    else:
        print("PROCESSO CONCLUÍDO. Seu código está limpo.")

if __name__ == "__main__":
    main()