import asyncio
import pandas as pd
from datetime import datetime
from pathlib import Path
from core.db import DatabaseManager
from core.logger import get_logger

logger = get_logger("ConabExactIngestor")

class ConabExactIngestor:
    """
    ETL Cirúrgico: Baseado na imagem fornecida do arquivo SojaSerieHist.xls.
    Correção v2: Ajuste de magnitude decimal e Sanity Check.
    """

    TARGET_STATES = ["MT", "PR"]
    TARGET_COL_NAME = "2023/24" 

    def __init__(self, file_path: str):
        self.db = DatabaseManager(use_service_role=True)
        self.file_path = Path(file_path)

    def _clean_number(self, value):
        """
        Limpeza robusta para formatos brasileiros mistos no Excel.
        """
        if pd.isna(value) or value == '-':
            return 0.0
        
        try:
            # Converte para string para limpar caracteres
            val_str = str(value).strip()
            
            # Remove pontos de milhar (3.697,5 -> 3697,5)
            val_str = val_str.replace('.', '')
            
            # Troca vírgula decimal por ponto (3697,5 -> 3697.5)
            val_str = val_str.replace(',', '.')
            
            float_val = float(val_str)
            
            # --- SANITY CHECK (A Correção do Erro de Magnitude) ---
            # A produtividade de soja no Brasil é ~3500 kg/ha.
            # Se o valor for > 10.000, provavelmente houve erro de casa decimal (ex: 30490).
            if float_val > 10000:
                float_val = float_val / 10.0
                
            return float_val
            
        except ValueError:
            return 0.0

    def process_file(self):
        logger.info(f"📂 Lendo arquivo: {self.file_path.name}")
        
        try:
            xls = pd.ExcelFile(self.file_path)
            
            # Busca aba de Produtividade
            target_sheet = None
            for sheet in xls.sheet_names:
                if "PRODUTIV" in sheet.upper() or "RENDIM" in sheet.upper():
                    target_sheet = sheet
                    break
            
            if not target_sheet:
                target_sheet = xls.sheet_names[0]

            logger.info(f"📑 Analisando aba: {target_sheet}")

            # Header Hunter
            df_raw = pd.read_excel(xls, sheet_name=target_sheet, header=None, nrows=20)
            header_idx = -1
            for i, row in df_raw.iterrows():
                row_str = " ".join([str(x) for x in row.values])
                if "REGIÃO/UF" in row_str and self.TARGET_COL_NAME in row_str:
                    header_idx = i
                    break
            
            if header_idx == -1:
                raise ValueError(f"Cabeçalho não encontrado.")

            # Leitura dos dados
            df = pd.read_excel(xls, sheet_name=target_sheet, header=header_idx)
            df.columns = [str(c).strip() for c in df.columns]

            records = []
            
            for _, row in df.iterrows():
                uf_raw = str(row['REGIÃO/UF']).strip().upper()
                
                if uf_raw in self.TARGET_STATES:
                    val_raw = row[self.TARGET_COL_NAME]
                    yield_val = self._clean_number(val_raw)
                    
                    if yield_val > 0:
                        records.append({
                            "state_code": uf_raw,
                            "crop_year": "2023/2024",
                            "commodity": "soja", # Coluna que estava faltando no cache
                            "yield_kg_ha": yield_val,
                            "prev_yield_kg_ha": 0.0,
                            "source": "CONAB_FILE_OFFICIAL",
                            "source_ref": f"{self.file_path.name}",
                            "ingested_at": datetime.utcnow().isoformat()
                        })
                        logger.info(f"   -> Extraído {uf_raw}: {yield_val} kg/ha")

            return records

        except Exception as e:
            logger.critical(f"Erro ao processar arquivo: {e}")
            return []

    async def run(self):
        if not self.file_path.exists():
            logger.error(f"Arquivo não encontrado: {self.file_path}")
            return

        records = self.process_file()
        
        if records:
            try:
                self.db.client.table("official_crop_stats").upsert(
                    records, on_conflict="state_code, crop_year, commodity"
                ).execute()
                logger.info(f"✅ Sucesso! {len(records)} registros corrigidos carregados.")
            except Exception as e:
                logger.error(f"Erro de persistência: {e}")
                logger.info("💡 DICA: Rode 'NOTIFY pgrst, 'reload schema';' no SQL Editor do Supabase.")
        else:
            logger.warning("Nenhum dado extraído.")

if __name__ == "__main__":
    FILE_NAME = "data/raw/SojaSerieHist.xls"
    ingestor = ConabExactIngestor(FILE_NAME)
    asyncio.run(ingestor.run())