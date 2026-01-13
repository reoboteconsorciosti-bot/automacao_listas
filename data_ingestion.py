import pandas as pd
import chardet
from data_cleaning import normalize_colname

# Colunas essenciais para cada tipo de estrutura
ASSERTIVA_ESSENTIAL_COLS = [
    "Razao", "Logradouro", "Numero", "Bairro", "Cidade", "UF", "CEP",
    "SOCIO1Nome", "SOCIO1Celular1", "SOCIO1Celular2"
]

LEMIT_ESSENTIAL_COLS = [
    "NOME", "Whats", "CEL", "DDD", "FONE"
]

def read_and_detect_encoding(file_obj):
    """Lê o conteúdo de um arquivo (ou UploadedFile) e detecta seu encoding."""
    if hasattr(file_obj, 'read'): # It's an UploadedFile or similar file-like object
        raw_data = file_obj.read()
        file_obj.seek(0) # Reset stream position for subsequent reads
    else: # Assume it's a filepath string
        try:
            with open(file_obj, 'rb') as f:
                raw_data = f.read()
        except FileNotFoundError:
            return None, None

    result = chardet.detect(raw_data)
    return raw_data, result['encoding'] or 'utf-8'

def infer_delimiter(file_obj, encoding):
    """Tenta inferir o delimitador de um arquivo CSV (ou UploadedFile)."""
    try:
        if hasattr(file_obj, 'read'): # It's an UploadedFile
            sample = file_obj.read(4096).decode(encoding, errors='ignore')
            file_obj.seek(0) # Reset stream position
        else: # Assume it's a filepath string
            with open(file_obj, 'r', encoding=encoding) as f:
                sample = f.read(4096)  # Lê uma amostra do arquivo

        delimiters = [';', ',', '\t', '|']
        counts = {d: sample.count(d) for d in delimiters}
        if not any(counts.values()):
            return ',' # Retorna um padrão se nenhum delimitador for encontrado
        return max(counts, key=counts.get)
    except Exception:
        return ',' # Retorna um padrão em caso de erro

def read_csv_smart(file_obj):
    """Lê um arquivo CSV (ou UploadedFile) com detecção inteligente de encoding e delimitador."""
    raw_data, encoding = read_and_detect_encoding(file_obj)
    if raw_data is None:
        print("DEBUG: read_csv_smart returning (empty df, file not found error)")
        return pd.DataFrame(), "Arquivo não encontrado ou ilegível."

    delimiter = infer_delimiter(file_obj, encoding)
    print(f"Inferred delimiter: {delimiter}")
    
    try:
        df = pd.read_csv(file_obj, delimiter=delimiter, encoding=encoding, on_bad_lines='warn')
        # Ensure column names are unique
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique():
            cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i, iid in enumerate(cols[cols == dup].index.values.tolist())]
        df.columns = cols
        print("DEBUG: read_csv_smart returning (df, None) - success path")
        return df, None
    except Exception as e:
        # Tenta com um encoding mais robusto como fallback
        try:
            if hasattr(file_obj, 'seek'): # For UploadedFile, reset position
                file_obj.seek(0)
            df = pd.read_csv(file_obj, delimiter=delimiter, encoding='latin-1', on_bad_lines='warn', mangle_dupe_cols=True)
            print("DEBUG: read_csv_smart returning (df, None) - fallback success path")
            return df, None
        except Exception as e_fallback:
            print(f"DEBUG: read_csv_smart returning (empty df, fallback error): {e_fallback}")
            return pd.DataFrame(), f"Erro ao ler CSV com ambos os engines: {e_fallback}"

def read_xlsx_smart(file_obj):
    """Lê um arquivo XLSX (ou UploadedFile), tentando várias abordagens."""
    try:
        # Tentativa padrão com o engine openpyxl
        df = pd.read_excel(file_obj, engine='openpyxl')
        print("DEBUG: read_xlsx_smart returning (df, None) - openpyxl success path")
        return df, None
    except Exception as e_openpyxl:
        # Fallback para o engine calamine se o openpyxl falhar
        try:
            if hasattr(file_obj, 'seek'): # For UploadedFile, reset position
                file_obj.seek(0)
            df = pd.read_excel(file_obj, engine='calamine')
            print("DEBUG: read_xlsx_smart returning (df, None) - calamine fallback success path")
            return df, None
        except Exception as e_calamine:
            print(f"DEBUG: read_xlsx_smart returning (empty df, calamine fallback error): {e_calamine}")
            return pd.DataFrame(), f"Erro ao ler XLSX com ambos os engines: openpyxl ({e_openpyxl}), calamine ({e_calamine})"

def load_data(file_input):
    """Carrega dados de um arquivo, seja CSV ou XLSX, e retorna um DataFrame, o tipo de estrutura e um erro (se houver).
    Aceita tanto filepath (string) quanto UploadedFile object.
    """
    print(f"DEBUG: load_data called with file_input type: {type(file_input)}")
    if file_input is None:
        print("DEBUG: load_data returning 3 values (None file_input)")
        return pd.DataFrame(), None, "Nenhum arquivo fornecido."

    # Determine the file extension
    if hasattr(file_input, 'name'): # It's an UploadedFile object
        file_extension = file_input.name.lower()
    else: # Assume it's a string filepath
        file_extension = str(file_input).lower()

    df = pd.DataFrame()
    err = None

    if file_extension.endswith('.csv'):
        df, err = read_csv_smart(file_input)
        print(f"DEBUG: read_csv_smart returned df shape: {df.shape if not df.empty else 'empty'}, err: {err}")
    elif file_extension.endswith('.xlsx'):
        df, err = read_xlsx_smart(file_input)
        print(f"DEBUG: read_xlsx_smart returned df shape: {df.shape if not df.empty else 'empty'}, err: {err}")
    else:
        print("DEBUG: load_data returning 3 values (unsupported file format)")
        return pd.DataFrame(), None, "Formato de arquivo não suportado. Use CSV ou XLSX."

    structure_type = None
    if err is None:
        # Tenta detectar o tipo de estrutura
        # Normaliza os nomes das colunas do DataFrame para comparação
        df_cols_normalized = {normalize_colname(col) for col in df.columns}
        print(f"DEBUG: Colunas do DataFrame normalizadas para detecção: {df_cols_normalized}")

        # --- Heurística Robusta de Detecção ---
        
        # 1. Lemit
        # Sinal Forte: Coluna 'POSSUI-WHATSAPP' (exclusiva do Lemit)
        has_possui_whatsapp = normalize_colname("POSSUI-WHATSAPP") in df_cols_normalized
        
        # Sinal Flexível: NOME + Pelo menos 1 campo de telefone típico do Lemit
        has_nome = normalize_colname("NOME") in df_cols_normalized
        lemit_phone_markers = ["Whats", "CEL", "FONE", "DDD", "Telefone", "Celular"]
        has_lemit_phone = any(normalize_colname(c) in df_cols_normalized for c in lemit_phone_markers)

        is_lemit_robust = has_possui_whatsapp or (has_nome and has_lemit_phone)

        # 2. Assertiva
        # Requer Razao (ou Nome) + quantidade mínima de outras colunas chaves
        has_razao = normalize_colname("Razao") in df_cols_normalized
        assertiva_markers = [c for c in ASSERTIVA_ESSENTIAL_COLS if c not in ["Razao", "SOCIO1Nome"]] # Remove nomes para verificar estrutura
        present_assertiva_markers = sum(1 for c in assertiva_markers if normalize_colname(c) in df_cols_normalized)
        
        is_assertiva_robust = (has_razao or has_nome) and present_assertiva_markers >= 3

        if is_lemit_robust:
            structure_type = "Lemit"
            print("DEBUG: Detectado como Lemit (Heurística Robusta)")
        elif is_assertiva_robust:
            structure_type = "Assertiva"
            print("DEBUG: Detectado como Assertiva (Heurística Robusta)")
        else:
             structure_type = "Desconhecida"

    print(f"DEBUG: load_data final return: df shape: {df.shape if not df.empty else 'empty'}, structure_type: {structure_type}, err: {err}")
    return df, structure_type, err

def save_temp_data(df):
    """Salva um DataFrame em um arquivo temporário."""
    temp_file = "temp_uploaded.csv"
    df.to_csv(temp_file, index=False)
    return temp_file

def read_temp_data():
    """Lê dados de um arquivo temporário."""
    temp_file = "temp_uploaded.csv"
    try:
        df = pd.read_csv(temp_file)
        return df, None
    except FileNotFoundError:
        return pd.DataFrame(), "Arquivo temporário não encontrado."

