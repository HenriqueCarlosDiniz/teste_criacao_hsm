import pandas as pd
import json

def corrigir_codificacao(texto):
    try:
        return texto.encode('latin').decode('utf-8')
    except (AttributeError, UnicodeEncodeError, UnicodeDecodeError):
        return texto

def contatos_para_lista(caminho_arquivo, n):
    # Verifica a extensão do arquivo
    if caminho_arquivo.endswith('.csv'):
        df = pd.read_csv(caminho_arquivo, sep=';',encoding='utf-8',engine='python')  # Define ';' como delimitador
    elif caminho_arquivo.endswith(('.xls', '.xlsx')):
        df = pd.read_excel(caminho_arquivo)
    else:
        raise ValueError("Formato de arquivo não suportado. Use CSV ou Excel.")

    # Seleciona as N primeiras linhas
    df = df.head(n)

    # Verifica se as colunas esperadas estão presentes
    colunas_esperadas = ["id", "nome", "telefone", "data ultima indicacao", "email", "link agendamento"]
    colunas_encontradas = df.columns.str.lower().str.strip().tolist()

    for coluna in colunas_esperadas:
        if coluna not in colunas_encontradas:
            raise ValueError(f"Coluna esperada '{coluna}' não encontrada. Colunas encontradas: {colunas_encontradas}")

    # Seleciona apenas as colunas necessárias
    df.columns = colunas_encontradas
    df = df[colunas_esperadas]

    # Renomeia as colunas para o formato esperado na saída
    df.columns = ["ID","NOME", "TELEFONE", "DATA_ULTIMA_INDICACAO", "EMAIL", "LINK_AGENDAMENTO"]
    #df["NOME"] = df["NOME"].apply(corrigir_codificacao)
    df["TELEFONE"] = pd.to_numeric(df["TELEFONE"], errors='coerce').fillna(0).astype('int64')

    # Converte para lista de dicionários
    contatos = df.to_dict(orient='records')

    return contatos

def salvar_lista_em_txt(lista_contatos, caminho_saida):
    # Converte a lista para uma string JSON
    lista_contatos_str = json.dumps(lista_contatos, ensure_ascii=False)
    with open(caminho_saida, 'w', encoding='utf-8') as f:
        f.write(f"{lista_contatos_str}\n")
# Exemplo de uso:
caminho_arquivo = "lista_paceiros_indicacao_maior_2022.csv"  # Ou "contatos.csv"
n = 50  # Número de contatos desejados
lista_contatos = contatos_para_lista(caminho_arquivo, n)

# Caminho do arquivo de saída
caminho_saida = "INDICA_PSD_INDICADORES.txt"
salvar_lista_em_txt(lista_contatos, caminho_saida)

print(f"Lista de contatos salva em {caminho_saida}")
