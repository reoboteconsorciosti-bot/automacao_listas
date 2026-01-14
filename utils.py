import io
import difflib
import pandas as pd
import numpy as np
from datetime import timedelta


def clean_phone_number(number_str, preserve_full=False):
    """Limpa e valida um número de telefone.

    Args:
        number_str: valor original (string/num).
        preserve_full: quando True, NÃO faz o corte final de últimos 10/11 dígitos
                       e em vez disso retorna todos os dígitos se houver ao menos 10.

    Retorna NaN quando inválido.
    """
    if pd.isna(number_str) or str(number_str).strip() == '':
        return np.nan
    digits = ''.join(filter(str.isdigit, str(number_str)))

    if preserve_full:
        if len(digits) >= 10:
            return digits
        return np.nan

    if len(digits) >= 11:
        return digits[-11:]
    if len(digits) == 10:
        return digits[-10:]
    return np.nan


def normalize_cep(cep_str):
    """Normaliza um CEP: remove não dígitos e retorna string com 8 dígitos ou empty string."""
    if pd.isna(cep_str) or str(cep_str).strip() == '':
        return ""
    digits = ''.join(filter(str.isdigit, str(cep_str)))
    if len(digits) == 8:
        return digits
    elif len(digits) > 8:
        return digits[-8:]
    else:
        return ""


def best_match_column(df_columns, candidates, min_score=50):
    """Retorna a melhor coluna de `df_columns` que corresponde aos `candidates`.
    Usa várias heurísticas combinadas (igualdade, substring, interseção de tokens e similaridade).
    Retorna string vazia se nenhuma coluna atingir `min_score`.
    """
    if not df_columns:
        return ''

    df_cols = [str(c) for c in df_columns]
    df_cols_lower = [c.lower() for c in df_cols]

    best_col = ''
    best_score = 0.0

    for cand in candidates:
        if not cand:
            continue
        cand_l = str(cand).lower()
        cand_tokens = set([t for t in ''.join(ch if ch.isalnum() else ' ' for ch in cand_l).split() if t])

        for i, col in enumerate(df_cols):
            col_l = df_cols_lower[i]
            score = 0.0

            if col_l == cand_l:
                score += 120

            if cand_l in col_l or col_l in cand_l:
                score += 80

            col_tokens = set([t for t in ''.join(ch if ch.isalnum() else ' ' for ch in col_l).split() if t])
            if cand_tokens and col_tokens:
                inter = cand_tokens.intersection(col_tokens)
                union = cand_tokens.union(col_tokens)
                if union:
                    score += 40 * (len(inter) / len(union))

            try:
                ratio = difflib.SequenceMatcher(a=cand_l, b=col_l).ratio()
                score += 40 * ratio
            except Exception:
                pass

            score -= 0.01 * len(col_l)

            if score > best_score:
                best_score = score
                best_col = col

    if best_score >= min_score:
        return best_col
    return ''


def proximo_dia_util(data_obj):
    """Retorna o próximo dia útil (pulando sábados e domingos)."""
    try:
        next_day = data_obj + timedelta(days=1)
        while next_day.weekday() >= 5:
            next_day += timedelta(days=1)
        return next_day
    except Exception:
        try:
            next_day = (pd.to_datetime(data_obj) + pd.Timedelta(days=1)).date()
            while next_day.weekday() >= 5:
                next_day = (pd.to_datetime(next_day) + pd.Timedelta(days=1)).date()
            return next_day
        except Exception:
            return data_obj


def determine_localidade(user_col_mapping, df_lote, default="CG"):
    possible_uf_keys = ["UF", "Estado", "Estado/UF", "UF/Estado"]
    for k in possible_uf_keys:
        uf_col = user_col_mapping.get(k)
        if uf_col and uf_col in df_lote.columns and not df_lote[uf_col].dropna().empty:
            val = str(df_lote[uf_col].iloc[0]).strip()
            if len(val) == 2:
                return val.upper()

    cidade_col = user_col_mapping.get("Cidade")
    if cidade_col and cidade_col in df_lote.columns and not df_lote[cidade_col].dropna().empty:
        val = str(df_lote[cidade_col].iloc[0]).strip()
        if 0 < len(val) <= 3:
            return val.upper()

    return default


def generate_excel_buffer(df, **kwargs):
    """
    Gera um buffer Excel em memória para um DataFrame.
    Aceita kwargs que são passados para to_excel (ex: sheet_name).
    O índice é False por padrão, mas pode ser sobrescrito via kwargs se necessário (embora implementado aqui fixo como index=False na chamada, poderiamos mudar).
    Na verdade, vamos garantir index=False e passar o resto.
    """
    output = io.BytesIO()
    try:
        # Se 'index' estiver em kwargs, usamos, caso contrário False
        index_arg = kwargs.pop('index', False)
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=index_arg, **kwargs)
        output.seek(0)
        return output
    except Exception:
        return io.BytesIO()

def format_phone_for_whatsapp_business(phone_str, default_country_code="+55", include_country_code=True):
    """
    Formata um número de telefone para o padrão WhatsApp Business (com DDI).
    Retorna uma tupla: (numero_formatado, status_msg)
    
    Status Msg pode ser: "OK", "VAZIO", "INVÁLIDO (Curto)", "CORRIGIDO (+55)", "INCERTO"
    
    Args:
        include_country_code: Se True, garante o +55. Se False, remove ou não adiciona.
    """
    if pd.isna(phone_str) or str(phone_str).strip() == "":
        return "", "VAZIO"

    # Limpeza básica
    cleaned = clean_phone_number(phone_str, preserve_full=True)
    if pd.isna(cleaned) or str(cleaned) == "":
        digits = ''.join(filter(str.isdigit, str(phone_str)))
        if not digits:
             return "", "VAZIO"
        cleaned = digits

    phone_clean = str(cleaned)
    raw_len = len(phone_clean)
    
    formatted_num = ""
    status = "OK"

    if raw_len < 10:
        # Número curto (sem DDD) - Descartar
        return "", "VAZIO"
    
    # Lógica se tiver país e quisermos remover
    if not include_country_code:
        # Se começar com 55 e tiver 12 ou 13 dígitos, remove o 55
        if phone_clean.startswith("55") and raw_len >= 12:
             formatted_num = phone_clean[2:]
             status = "OK (Sem +55)"
        else:
             formatted_num = phone_clean
             status = "OK (Sem +55)"
        return formatted_num, status

    # Lógica com país (Padrão)
    if phone_clean.startswith("55") and raw_len >= 12:
        # Já tem DDI (55 + 2 DDD + 8/9 num = 12/13 digitos)
        formatted_num = f"+{phone_clean}"
        status = "OK"
    
    elif raw_len == 10 or raw_len == 11:
        # Caso padrão DDD+Num (10 ou 11 digitos)
        formatted_num = f"{default_country_code}{phone_clean}"
        status = "CORRIGIDO (+55)"
        
    else:
        # Outros casos (ex: muito longo sem 55) mas com pelo menos 10 digitos
        # Tenta garantir o +55 se não tiver
        if not phone_clean.startswith("55"):
             formatted_num = f"{default_country_code}{phone_clean}"
        else:
             formatted_num = f"+{phone_clean}"
        status = "INCERTO"

    return formatted_num, status


def clean_phone_number(number_str, preserve_full=False):
    """Limpa e valida um número de telefone.

    Args:
        number_str: valor original (string/num).
        preserve_full: quando True, NÃO faz o corte final de últimos 10/11 dígitos
                       e em vez disso retorna todos os dígitos se houver ao menos 10.

    Retorna NaN quando inválido.
    """
    if pd.isna(number_str) or str(number_str).strip() == '':
        return np.nan
    digits = ''.join(filter(str.isdigit, str(number_str)))

    if preserve_full:
        # Preserva o valor inteiro quando parece um telefone (>=10 dígitos)
        if len(digits) >= 10:
            return digits
        return np.nan

    # Normalização robusta padrão (para WhatsApp/negócios):
    # - Se houver 11 ou mais dígitos, assume os últimos 11 correspondem a DDD + número móvel.
    # - Se houver exatamente 10 dígitos, assume DDD + número fixo/antigo (sem nono dígito).
    # - Caso contrário, considera inválido.
    if len(digits) >= 11:
        return digits[-11:]
    if len(digits) == 10:
        return digits[-10:]
    return np.nan


def normalize_cep(cep_str):
    """Normaliza um CEP: remove não dígitos e retorna string com 8 dígitos ou empty string."""
    if pd.isna(cep_str) or str(cep_str).strip() == '':
        return ""
    digits = ''.join(filter(str.isdigit, str(cep_str)))
    if len(digits) == 8:
        # Retorna apenas os 8 dígitos (sem traço)
        return digits
    elif len(digits) > 8:
        # Se tiver mais dígitos, pega os 8 últimos (possível prefixo extra)
        d = digits[-8:]
        return d
    else:
        # Retorna vazio para CEPs inválidos/curtos
        return ""


def best_match_column(df_columns, candidates, min_score=50):
    """Retorna a melhor coluna de `df_columns` que corresponde aos `candidates`.
    Usa várias heurísticas combinadas (igualdade, substring, interseção de tokens e similaridade).
    Retorna string vazia se nenhuma coluna atingir `min_score`.
    """
    if not df_columns:
        return ''

    df_cols = [str(c) for c in df_columns]
    df_cols_lower = [c.lower() for c in df_cols]

    best_col = ''
    best_score = 0.0

    for cand in candidates:
        if not cand:
            continue
        cand_l = str(cand).lower()
        cand_tokens = set([t for t in ''.join(ch if ch.isalnum() else ' ' for ch in cand_l).split() if t])

        for i, col in enumerate(df_cols):
            col_l = df_cols_lower[i]
            score = 0.0

            # Exata igualdade (maior peso)
            if col_l == cand_l:
                score += 120

            # Substring (col contém candidato ou candidato contém coluna)
            if cand_l in col_l or col_l in cand_l:
                score += 80

            # Token overlap
            col_tokens = set([t for t in ''.join(ch if ch.isalnum() else ' ' for ch in col_l).split() if t])
            if cand_tokens and col_tokens:
                inter = cand_tokens.intersection(col_tokens)
                union = cand_tokens.union(col_tokens)
                if union:
                    score += 40 * (len(inter) / len(union))

            # Similaridade fuzzier via SequenceMatcher
            try:
                ratio = difflib.SequenceMatcher(a=cand_l, b=col_l).ratio()
                score += 40 * ratio
            except Exception:
                pass

            # Slight preference for shorter column names on ties
            score -= 0.01 * len(col_l)

            if score > best_score:
                best_score = score
                best_col = col

    if best_score >= min_score:
        return best_col
    return ''


def proximo_dia_util(data_obj):
    """Retorna o próximo dia útil (pulando sábados e domingos)."""
    try:
        next_day = data_obj + timedelta(days=1)
        while next_day.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
            next_day += timedelta(days=1)
        return next_day
    except Exception:
        # Se qualquer erro ocorrer (ex: data_obj não é date), tente converter
        try:
            next_day = (pd.to_datetime(data_obj) + pd.Timedelta(days=1)).date()
            while next_day.weekday() >= 5:
                next_day = (pd.to_datetime(next_day) + pd.Timedelta(days=1)).date()
            return next_day
        except Exception:
            return data_obj


def determine_localidade(user_col_mapping, df_lote, default="CG"):
    """Determina uma string de localidade segura para uso em nomes de arquivos.

    Regras:
    - Prefere coluna 'UF' quando mapeada e a célula parece ser a sigla (2 letras).
    - Caso contrário, usa 'Cidade' apenas se for muito curta (<=3 chars).
    - Caso contrário, retorna `default`.
    """
    # Tenta várias chaves comuns para UF
    possible_uf_keys = ["UF", "Estado", "Estado/UF", "UF/Estado"]
    for k in possible_uf_keys:
        uf_col = user_col_mapping.get(k)
        if uf_col and uf_col in df_lote.columns and not df_lote[uf_col].dropna().empty:
            val = str(df_lote[uf_col].iloc[0]).strip()
            if len(val) == 2:
                return val.upper()

    # Se não houver UF válido, verificar Cidade mas somente se curta (evita nomes longos como 'DOURADOS')
    cidade_col = user_col_mapping.get("Cidade")
    if cidade_col and cidade_col in df_lote.columns and not df_lote[cidade_col].dropna().empty:
        val = str(df_lote[cidade_col].iloc[0]).strip()
        if 0 < len(val) <= 3:
            return val.upper()

    return default


def gerar_excel_em_memoria(df_lote, consultor, data):
    """Gera um buffer Excel em memória para um DataFrame (usado por divisor de listas)."""
    output = io.BytesIO()
    try:
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_lote.to_excel(writer, index=False)
        output.seek(0)
        return output
    except Exception:
        return io.BytesIO()
