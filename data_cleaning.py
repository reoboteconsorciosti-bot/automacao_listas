import pandas as pd
import unicodedata
import logging
import numpy as np
from typing import List, Dict, Any, Union, Tuple
from utils import best_match_column
from utils import format_phone_for_whatsapp_business

# Ordem final das colunas de saída
# Ordem final das colunas de saída
FIXED_OUTPUT_ORDER = [
    "Razao", "Logradouro", "Numero", "Bairro", "Cidade", "UF",
    "NOME", "Whats", "CEL"
]

# Colunas para extração completa (Superset de tudo que queremos buscar)
FULL_EXTRACTION_COLS = [
    "Razao", "Logradouro", "Numero", "Bairro", "Cidade", "UF", "CEP", "CNPJ",
    "SOCIO1Nome", "SOCIO1Celular1", "SOCIO1Celular2",
    "NOME", "Whats", "CEL", "DDD", "FONE"
]

def normalize_colname(name: Any) -> str:
    """Remove acentos, espaços e converte para minúsculas."""
    if name is None:
        return ""
    nfkd = unicodedata.normalize('NFKD', str(name))
    return ''.join([c for c in nfkd if not unicodedata.combining(c)]).replace(' ', '').lower()

def map_essential_columns(df: pd.DataFrame, essential_cols: List[str]) -> Dict[str, str]:
    """Mapeia nomes de colunas normalizados para os nomes originais."""
    norm_to_orig = {normalize_colname(col): col for col in df.columns}
    found = {}
    for col in essential_cols:
        norm = normalize_colname(col)
        if norm in norm_to_orig:
            found[col] = norm_to_orig[norm]
    return found

def _clean_phone_number(number_str: Any) -> Union[str, float]:
    """Limpa e valida um número de telefone, retornando NaN se inválido."""
    if pd.isna(number_str) or str(number_str).strip() == '':
        return np.nan
    cleaned = ''.join(filter(str.isdigit, str(number_str)))
    if not cleaned:
        return np.nan
    return cleaned

def _format_phone_with_ddd(phone_str, include_country_code=False):
    """Formata um número de telefone limpo com DDD e opcionalmente +55."""
    if pd.isna(phone_str) or not isinstance(phone_str, str):
        return np.nan
    cleaned = ''.join(filter(str.isdigit, phone_str))
    if len(cleaned) < 10: # Mínimo 2 dígitos para DDD + 8 para o número
        return np.nan

    ddd = cleaned[:2]
    number = cleaned[2:]

    # Formata a parte do número: XXXXX-XXXX (9 dígitos) ou XXXX-XXXX (8 dígitos)
    if len(number) == 9:
        formatted_number = f"{number[:5]}-{number[5:]}"
    elif len(number) == 8:
        formatted_number = f"{number[:4]}-{number[4:]}"
    else:
        return np.nan # Caso o número não tenha 8 ou 9 dígitos após o DDD

    if include_country_code:
        return f"+55 {ddd} {formatted_number}"
    else:
        return f"{ddd} {formatted_number}"

def _is_valid_cpf(cpf_str):
    """Valida se a string é um CPF de 11 dígitos (apenas números)."""
    if pd.isna(cpf_str) or not isinstance(cpf_str, str):
        return False
    cleaned = ''.join(filter(str.isdigit, cpf_str))
    return len(cleaned) == 11

def identify_structure(df, ASSERTIVA_ESSENTIAL_COLS, LEMIT_ESSENTIAL_COLS):
    """Identifica a estrutura do DataFrame (Assertiva ou Lemit)."""
    norm_cols = {normalize_colname(col) for col in df.columns}
    
    # Contar quantas colunas essenciais de cada tipo estão presentes
    assertiva_matches = sum(1 for col in ASSERTIVA_ESSENTIAL_COLS if normalize_colname(col) in norm_cols)
    lemit_matches = sum(1 for col in LEMIT_ESSENTIAL_COLS if normalize_colname(col) in norm_cols)

    # Decidir com base na contagem de correspondências
    if lemit_matches > assertiva_matches:
        return "Lemit"
    else:
        return "Assertiva"

def clean_and_filter_data(df: pd.DataFrame, essential_cols: List[str]) -> Tuple[pd.DataFrame, List[str], str]:
    if df.empty:
        logging.warning("DataFrame de entrada está vazio.")
        print("DEBUG: clean_and_filter_data returning (empty df, empty missing, Unknown structure) - df.empty path")
        return pd.DataFrame(), [], "Unknown"

    # A estrutura agora é passada como argumento ou detectada em load_data
    # Removendo a chamada identify_structure(df) daqui
    # structure = identify_structure(df)
    # logging.info(f"Estrutura de dados identificada: {structure}")

    df_processed = pd.DataFrame()

    # Mapeamento de nomes padrão para possíveis nomes de colunas de origem
    MAPPING = {
        "Razao": ["Razao", "RAZAO_SOCIAL", "NOME/RAZAO_SOCIAL", "Fantasia"],
        "SOCIO1Nome": ["SOCIO1Nome", "NOME"],
        "Logradouro": ["Logradouro", "FULL-LOGRADOURO", "FULL_LOGRADOURO", "Endereco", "ENDERECO_COMPLETO", "TIPO-LOGRADOURO"],
        "Numero": ["Numero", "NUMERO"],
        "Bairro": ["Bairro", "BAIRRO"],
        "Cidade": ["Cidade", "CIDADE"],
        "UF": ["UF", "ESTADO"],
        "CNPJ": ["CNPJ", "CPF/CNPJ"],
        "Whats": ["Whats", "WhatsApp", "Telefone", "Celular", "Contato", "POSSUI-WHATSAPP"],
        "CEL": ["CEL", "Celular", "Telefone", "Whats", "WhatsApp"],
        "DDD": ["DDD", "TELEFONE_DDD", "FONE_DDD"],
        "FONE": ["FONE", "TELEFONE_NUMERO", "FONE_NUMERO", "NUMERO_TELEFONE"]
    }

    # Constrói o DataFrame processado de forma segura, coluna por coluna
    for std_col in essential_cols:
        source_options = MAPPING.get(std_col, [std_col]) # Usa o nome da coluna essencial como fallback
        found_valid_col = False
        
        # Para colunas como Logradouro, Numero, Bairro, Cidade, UF, procure por variações com sufixos numéricos
        potential_source_cols = []
        if std_col == "Logradouro":
            potential_source_cols = [col for col in ["Logradouro", "FULL-LOGRADOURO", "Logradouro.1", "FULL-LOGRADOURO.1", "Logradouro.2", "FULL-LOGRADOURO.2", "Logradouro.3", "FULL-LOGRADOURO.3"] if col in df.columns]
        elif std_col == "Numero":
            potential_source_cols = [col for col in ["NUMERO", "NUMERO.1", "NUMERO.2", "NUMERO.3"] if col in df.columns]
        elif std_col == "Bairro":
            potential_source_cols = [col for col in ["BAIRRO", "BAIRRO.1", "BAIRRO.2", "BAIRRO.3"] if col in df.columns]
        elif std_col == "Cidade":
            potential_source_cols = [col for col in ["CIDADE", "CIDADE.1", "CIDADE.2", "CIDADE.3"] if col in df.columns]
        elif std_col == "UF":
            potential_source_cols = [col for col in ["UF", "UF.1", "UF.2", "UF.3"] if col in df.columns]
        else:
            potential_source_cols = [col for col in source_options if col in df.columns]

        # Ensure the order is maintained (base first, then numbered)
        potential_source_cols.sort(key=lambda x: (len(x), x))

        # Tenta encontrar a coluna diretamente
        for source_col in potential_source_cols:
            if source_col in df.columns:
                col_data = df[source_col].astype(str).str.strip()
                logging.debug(f"[DEBUG_MAP] Coluna '{std_col}': Tentando '{source_col}'. Conteúdo (primeiras 5): {col_data.head().tolist()}. Any non-empty: {col_data.any()}")
                # Check if the column has any non-null/non-empty values (after stripping whitespace)
                if col_data.any():
                    df_processed[std_col] = df[source_col]
                    found_valid_col = True
                    logging.info(f"Coluna '{std_col}' mapeada de '{source_col}' com dados.")
                    break 
        
        # Se não encontrou diretamente, tenta fuzzy match com as opções
        if not found_valid_col:
            # Reúne todos os candidatos (source_options + potential extras)
            candidates = source_options + potential_source_cols
            best_col = best_match_column(df.columns.tolist(), candidates, min_score=60)
            
            if best_col:
                 col_data = df[best_col].astype(str).str.strip()
                 if col_data.any():
                    df_processed[std_col] = df[best_col]
                    found_valid_col = True
                    logging.info(f"Coluna '{std_col}' mapeada de '{best_col}' via fuzzy match.")
        
        if not found_valid_col:
            logging.warning(f"Nenhuma coluna válida encontrada para '{std_col}' entre as opções: {potential_source_cols}. Definindo como NaN.")
            df_processed[std_col] = np.nan

    logging.info("DataFrame após mapeamento inicial de colunas:")
    logging.info(df_processed.head())
    
    # Inicializa colunas de celular como string para evitar FutureWarnings
    # Apenas inicializa se elas estiverem nas essential_cols
    # Inicializa colunas de celular como string para evitar FutureWarnings
    # Apenas inicializa se elas NÃO existirem ainda
    if "SOCIO1Celular1" in essential_cols and "SOCIO1Celular1" not in df_processed.columns:
        df_processed["SOCIO1Celular1"] = pd.Series(dtype='object')
    if "SOCIO1Celular2" in essential_cols and "SOCIO1Celular2" not in df_processed.columns:
        df_processed["SOCIO1Celular2"] = pd.Series(dtype='object')
    if "Whats" in essential_cols and "Whats" not in df_processed.columns:
        df_processed["Whats"] = pd.Series(dtype='object')
    if "CEL" in essential_cols and "CEL" not in df_processed.columns:
        df_processed["CEL"] = pd.Series(dtype='object')
        
    # Garantir que colunas existentes sejam object para evitar warnings
    for c in ["SOCIO1Celular1", "SOCIO1Celular2", "Whats", "CEL"]:
        if c in df_processed.columns:
             df_processed[c] = df_processed[c].astype('object')

    # --- Lógica dedicada para SOCIO1Celular1 e SOCIO1Celular2 / DDD/FONE/Whats/CEL ---

    # Prioriza a combinação DDD + FONE/CEL para Lemit, ou usa colunas diretas para Assertiva
    # A lógica de detecção de estrutura foi movida para load_data, então precisamos do structure_type aqui
    # Para simplificar, vamos assumir que se essential_cols contém DDD/FONE, é Lemit-like
    is_lemit_like = "DDD" in essential_cols or "FONE" in essential_cols

    if is_lemit_like:
        # Tenta encontrar até 2 números de telefone válidos combinando DDD e FONE/CEL
        for index, row in df.iterrows():
            valid_phones = []
            # Itera sobre as possíveis combinações de DDD e FONE/CEL
            for i in range(8): # DDD, DDD.1, ..., DDD.7 e FONE, FONE.1, ..., FONE.7
                ddd_col = f"DDD.{i}" if i > 0 else "DDD"
                fone_col = f"FONE.{i}" if i > 0 else "FONE"
                cel_col = f"CEL.{i}" if i > 0 else "CEL"

                ddd_val = str(row.get(ddd_col, '')).strip()
                fone_val = str(row.get(fone_col, '')).strip()
                cel_val = str(row.get(cel_col, '')).strip()

                logging.debug(f"[DEBUG] Linha {index}, Tentando cols: DDD={ddd_col} ({ddd_val}), FONE={fone_col} ({fone_val}), CEL={cel_col} ({cel_val})")

                # Tenta combinar DDD com FONE
                if ddd_val and fone_val:
                    combined_phone = ddd_val + fone_val
                    cleaned_phone = _clean_phone_number(combined_phone)
                    logging.debug(f"[DEBUG] Combinado DDD+FONE: {combined_phone}, Limpo: {cleaned_phone}")
                    if pd.notna(cleaned_phone) and cleaned_phone != "":
                        valid_phones.append(cleaned_phone)
                
                # Tenta combinar DDD com CEL
                if ddd_val and cel_val:
                    combined_phone = ddd_val + cel_val
                    cleaned_phone = _clean_phone_number(combined_phone)
                    logging.debug(f"[DEBUG] Combinado DDD+CEL: {combined_phone}, Limpo: {cleaned_phone}")
                    if pd.notna(cleaned_phone) and cleaned_phone != "":
                        valid_phones.append(cleaned_phone)

                # Se FONE ou CEL vierem sozinhos e forem válidos (já com DDD)
                if not ddd_val and fone_val:
                    cleaned_phone = _clean_phone_number(fone_val)
                    logging.debug(f"[DEBUG] FONE sozinho: {fone_val}, Limpo: {cleaned_phone}")
                    if pd.notna(cleaned_phone) and cleaned_phone != "":
                        valid_phones.append(cleaned_phone)
                if not ddd_val and cel_val:
                    cleaned_phone = _clean_phone_number(cel_val)
                    logging.debug(f"[DEBUG] CEL sozinho: {cel_val}, Limpo: {cleaned_phone}")
                    if pd.notna(cleaned_phone) and cleaned_phone != "":
                        valid_phones.append(cleaned_phone)

                if len(valid_phones) >= 2: # Já encontrou 2, pode parar de procurar para esta linha
                    break
            
            # Atribui os telefones encontrados
            if len(valid_phones) > 0:
                if "SOCIO1Celular1" in essential_cols:
                    df_processed.at[index, "SOCIO1Celular1"] = valid_phones[0]
                elif "Whats" in essential_cols: # Para Lemit, Whats é o principal
                    df_processed.at[index, "Whats"] = valid_phones[0]

            if len(valid_phones) > 1:
                if "SOCIO1Celular2" in essential_cols:
                    df_processed.at[index, "SOCIO1Celular2"] = valid_phones[1]
                elif "CEL" in essential_cols: # Para Lemit, CEL é o secundário
                    df_processed.at[index, "CEL"] = valid_phones[1]

    else: # Estrutura Assertiva ou desconhecida, usa as colunas diretas
        for index, row in df.iterrows():
            if "SOCIO1Celular1" in essential_cols:
                s1_cel1 = row.get("SOCIO1Celular1", np.nan)
                df_processed.at[index, "SOCIO1Celular1"] = _clean_phone_number(s1_cel1)
            
            if "SOCIO1Celular2" in essential_cols:
                s1_cel2 = row.get("SOCIO1Celular2", np.nan)
                df_processed.at[index, "SOCIO1Celular2"] = _clean_phone_number(s1_cel2)

    logging.info("DataFrame após tratamento de telefones dedicados:")
    logging.info(df_processed.head())

    # --- Aplica a formatação final dos números de celular (Centralizada) ---
    # Agora usamos format_phone_for_whatsapp_business que retorna (formatted, status)
    # Pegamos apenas o [0] (formatted). Se for VAZIO, fica string vazia.
    
    if "SOCIO1Celular1" in essential_cols:
        df_processed["SOCIO1Celular1"] = df_processed["SOCIO1Celular1"].apply(lambda x: format_phone_for_whatsapp_business(x)[0])
    if "SOCIO1Celular2" in essential_cols:
        df_processed["SOCIO1Celular2"] = df_processed["SOCIO1Celular2"].apply(lambda x: format_phone_for_whatsapp_business(x)[0])
    if "Whats" in essential_cols:
        df_processed["Whats"] = df_processed["Whats"].apply(lambda x: format_phone_for_whatsapp_business(x)[0])
    if "CEL" in essential_cols:
        df_processed["CEL"] = df_processed["CEL"].apply(lambda x: format_phone_for_whatsapp_business(x)[0])

    logging.info("DataFrame após formatação final dos celulares (Centralizada):")
    logging.info(df_processed.head())

    # --- Lógica de Fallback para Sócios (apenas para Assertiva-like) ---
    # Se as colunas de sócio estiverem nas essential_cols, aplica a lógica de fallback
    if "SOCIO1Nome" in essential_cols:
        SOCIO_FIELDS = [
            ("Nome", "SOCIO1Nome", "SOCIO2Nome"),
            ("Celular1", "SOCIO1Celular1", "SOCIO2Celular1"),
            ("Celular2", "SOCIO1Celular2", "SOCIO2Celular2"),
            ("CPF", "SOCIO1CPF", "SOCIO2CPF")
        ]

        def _is_field_valid(field_name, value):
            if pd.isna(value) or str(value).strip() == "":
                return False
            if field_name == "CPF":
                return _is_valid_cpf(str(value))
            return True

        rows_to_drop = []
        for index, row in df_processed.iterrows():
            socio1_has_any_valid_data = False
            
            for field_name, s1_col, s2_col in SOCIO_FIELDS:
                s1_val = row.get(s1_col, np.nan)
                s2_val = row.get(s2_col, np.nan)

                s1_valid = _is_field_valid(field_name, s1_val)
                s2_valid = _is_field_valid(field_name, s2_val)

                if not s1_valid:
                    if s2_valid:
                        if s1_col in df_processed.columns:
                            df_processed.at[index, s1_col] = s2_val
                            logging.info(f"[FALLBACK] {s1_col} inválido para linha {index}. Usando {s2_col} ({s2_val}).")
                            socio1_has_any_valid_data = True
                    else:
                        if s1_col in df_processed.columns:
                            df_processed.at[index, s1_col] = np.nan # Marcar como NaN se nenhum for válido
                            logging.warning(f"[FALLBACK] {s1_col} e {s2_col} inválidos/ausentes para linha {index}. Definido {s1_col} como NaN.")
                else:
                    socio1_has_any_valid_data = True
            
            # After checking all fields, decide if the row should be dropped
            if not socio1_has_any_valid_data:
                logging.error(f"[ERRO] Nenhum sócio com dados válidos encontrado para linha {index} após fallbacks. Marcando para remoção.")
                rows_to_drop.append(index)
        
        if rows_to_drop:
            df_processed.drop(rows_to_drop, inplace=True)
            logging.info(f"Removidas {len(rows_to_drop)} linhas sem sócios válidos após fallbacks.")

        # Remover colunas SOCIO2* após o fallback
        cols_to_drop_socio2 = [col for col in df_processed.columns if col.startswith("SOCIO2")]
        if cols_to_drop_socio2:
            df_processed.drop(columns=cols_to_drop_socio2, inplace=True)
            logging.info(f"Colunas SOCIO2 removidas: {cols_to_drop_socio2}")

        logging.info("DataFrame após tratamento de telefone e fallback de sócios:")
        logging.info(df_processed.head())

    # --- Bloco de Limpeza e Seleção (Unificado) ---
    
    # --- Unificação de Colunas (SOCIO -> NOME/Whats/CEL) ---
    # Se existirem colunas de SOCIO preenchidas, movemos para NOME/Whats/CEL se estes estiverem vazios
    
    # Converte strings vazias para NaN para o fillna funcionar
    cols_to_fix = ["NOME", "Whats", "CEL", "SOCIO1Nome", "SOCIO1Celular1", "SOCIO1Celular2"]
    for col in cols_to_fix:
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].replace(r'^\s*$', np.nan, regex=True)

    if "SOCIO1Nome" in df_processed.columns:
        df_processed["NOME"] = df_processed["NOME"].fillna(df_processed["SOCIO1Nome"])
    
    if "SOCIO1Celular1" in df_processed.columns:
        df_processed["Whats"] = df_processed["Whats"].fillna(df_processed["SOCIO1Celular1"])
        
    if "SOCIO1Celular2" in df_processed.columns:
        df_processed["CEL"] = df_processed["CEL"].fillna(df_processed["SOCIO1Celular2"])

    # Remove duplicates com base APENAS no Whats final formatado
    # Primeiro remove vazios
    df_processed = df_processed[df_processed["Whats"] != ""]
    df_processed = df_processed[df_processed["Whats"].notna()]
    
    # Remove duplicatas de telefone
    if not df_processed.empty:
        df_processed.drop_duplicates(subset=["Whats"], keep='first', inplace=True)

    # Limpeza final das colunas de texto
    for col in ["Razao", "Logradouro", "Bairro", "Cidade", "UF", "NOME", "Whats", "CEL"]:
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].fillna('').astype(str).str.strip()

    # Seleciona e ordena as colunas para a saída final
    # Garante que todas as colunas de FIXED_OUTPUT_ORDER existam no final, mesmo que vazias
    for col in FIXED_OUTPUT_ORDER:
        if col not in df_processed.columns:
            df_processed[col] = "" # Preenche com vazio se não existir

    final_cols = [col for col in FIXED_OUTPUT_ORDER] # Usa a ordem fixa completa
    df_final = df_processed[final_cols].copy()

    # Ordena o resultado final
    sort_cols = [col for col in ["Bairro", "Razao"] if col in df_final.columns]
    if sort_cols:
        df_final.sort_values(by=sort_cols, ascending=True, inplace=True)

    missing = [col for col in essential_cols if col not in df_processed.columns]
    logging.info("DataFrame final antes de retornar:")
    logging.info(df_final.head())

    print(f"DEBUG: clean_and_filter_data final return: df_final shape: {df_final.shape if not df_final.empty else 'empty'}, missing: {missing}, structure: {'Structure_Type_Placeholder'}")
    return df_final.reset_index(drop=True), missing, "Structure_Type_Placeholder" # Retorna 3 valores
