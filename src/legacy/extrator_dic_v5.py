import pdfplumber
import json
import re
from pathlib import Path

PDF_PATH = "metadados/DIC_DADOS_Meningite_v5.pdf"
OUTPUT_JSON = "metadados/dicionario_v5_mapeado.json"

def extrair_v5_alta_precisao():
    print(f"🚀 Treinando extrator em todas as 23 páginas de {PDF_PATH}...")
    mapeamento = {}
    
    with pdfplumber.open(PDF_PATH) as pdf:
        for i, pagina in enumerate(pdf.pages):
            texto = pagina.extract_text()
            if not texto: continue
            
            # Estratégia hibrida: Tenta extrair a tabela primeiro
            tabela = pagina.extract_table()
            if tabela:
                for linha in tabela:
                    # Filtro para identificar a coluna DBF (normalmente a última ou penúltima)
                    # Procuramos por um nome em caixa alta com underscores (Ex: ID_AGRAVO)
                    for celula in linha:
                        if celula and re.match(r'^[A-Z][A-Z0-9_]{2,15}$', celula.strip()):
                            var_name = celula.strip()
                            # Tenta capturar o label (geralmente na 1ª ou 2ª coluna)
                            label = linha[1] if len(linha) > 1 else "Label não capturado"
                            
                            # Extração de fatores via regex no texto da linha ou célula de categorias
                            categorias_raw = linha[3] if len(linha) > 3 else ""
                            fatores = dict(re.findall(r"(\d+)\s*[-–]\s*([^0-9\n]+)", str(categorias_raw)))
                            
                            mapeamento[var_name] = {
                                "label": str(label).strip(),
                                "fatores": fatores,
                                "pagina": i + 1
                            }

    # Salva o JSON para avaliação
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(mapeamento, f, indent=4, ensure_ascii=False)
    
    print(f"🎯 Extração Finalizada: {len(mapeamento)} variáveis encontradas.")

if __name__ == "__main__":
    extrair_v5_alta_precisao()
