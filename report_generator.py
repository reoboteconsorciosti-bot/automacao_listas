import streamlit as st
import json
import utils
from utils import process_agendor_report, format_phone_for_whatsapp_business, generate_excel_buffer, clean_phone_number, normalize_cep, best_match_column, proximo_dia_util, determine_localidade
from streamlit_option_menu import option_menu
import pandas as pd
import os
from datetime import datetime, date, timedelta
import io
import zipfile
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
import warnings
import numpy as np
import glob
import difflib

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')

import logging

# Ensure logs directory exists before configuring logging
try:
    os.makedirs('logs', exist_ok=True)
except Exception:
    pass

# Logging setup (logs/app.log)
logging.basicConfig(
    filename=os.path.join('logs', 'app.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Persistência dinâmica de consultores e equipes ---
# --- Persistência dinâmica de consultores e equipes ---
# Configuração de diretório de dados (para persistência em Docker)
DATA_DIR = os.getenv("DATA_DIR", ".")
os.makedirs(DATA_DIR, exist_ok=True) # Garante que a pasta exista

CONSULTORES_FILE = os.path.join(DATA_DIR, "consultores.json")
EQUIPES_FILE = os.path.join(DATA_DIR, "equipes.json")

# Função de inicialização de segurança (Self-healing)
def init_db():
    if not os.path.exists(CONSULTORES_FILE):
        with open(CONSULTORES_FILE, "w", encoding="utf-8") as f:
            json.dump([], f)
    
    if not os.path.exists(EQUIPES_FILE):
        with open(EQUIPES_FILE, "w", encoding="utf-8") as f:
            json.dump({"equipes": []}, f)

# Inicializa DB na importação
init_db()

def carregar_consultores():
    try:
        with open(CONSULTORES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def salvar_consultores(consultores):
    with open(CONSULTORES_FILE, "w", encoding="utf-8") as f:
        json.dump(consultores, f, ensure_ascii=False, indent=2)

def carregar_equipes():
    try:
        with open(EQUIPES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)["equipes"]
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return []

def salvar_equipes(equipes):
    with open(EQUIPES_FILE, "w", encoding="utf-8") as f:
        json.dump({"equipes": equipes}, f, ensure_ascii=False, indent=2)

from data_ingestion import load_data, ASSERTIVA_ESSENTIAL_COLS, LEMIT_ESSENTIAL_COLS
from data_cleaning import clean_and_filter_data, FULL_EXTRACTION_COLS
from create_pdf import create_pdf_robust

# --- Configurações e Lógica para o Divisor de Listas ---

# Cores para o Excel (RGB para OpenPyXL)
COLOR_LIGHT_BLUE = "E0EBFB"
COLOR_WHITE = "FFFFFF"

from utils import (
    clean_phone_number,
    normalize_cep,
    best_match_column,
    proximo_dia_util,
    determine_localidade,
    generate_excel_buffer,
    format_phone_for_whatsapp_business,
)


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


 

def aba_higienizacao():
    # Garante que as variáveis de sessão estejam inicializadas
    if "structure_type" not in st.session_state:
        st.session_state.structure_type = "Não Detectada"
    if "df_clean" not in st.session_state:
        st.session_state.df_clean = pd.DataFrame()
    if "missing_cols" not in st.session_state:
        st.session_state.missing_cols = []

    st.header("Higienização e Geração de Listas - Assertiva e Lemit")
    st.info("Faça o upload de um arquivo enriquecido do Lemit ou Assertiva, o retorno será uma lista formatada pdf e o arquivo xlsx.")
    uploaded_file = st.file_uploader("Faça upload do arquivo CSV Assertiva ou Lemit", type=["csv"], key="higienizacao_uploader")
    
    if uploaded_file:
            df_raw, structure_type, err = load_data(uploaded_file)
            if err:
                st.error(err)
                return
            
            # DEBUG: Imprime as colunas do DataFrame carregado
            print(f"DEBUG: Colunas do DataFrame carregado: {df_raw.columns.tolist()}")

            st.session_state.structure_type = structure_type # Atualiza o valor após a detecção

            st.success(f"Planilha {st.session_state.structure_type} Detectada")

            # Determina as colunas essenciais com base na estrutura detectada (para LOG)
            if st.session_state.structure_type == "Assertiva":
                st.info(f"Usando colunas de extração Assertiva.")
            elif st.session_state.structure_type == "Lemit":
                 st.info(f"Usando colunas de extração Lemit.")
            else:
                st.error("Estrutura de planilha desconhecida. Não é possível prosseguir com a higienização.")
                st.session_state.df_clean = pd.DataFrame() # Garante que df_clean seja um DataFrame vazio
                st.session_state.missing_cols = [] # Garante que missing_cols seja uma lista vazia
                return # Sai da função para evitar o erro de desempacotamento

            # Chama clean_and_filter_data com FULL_EXTRACTION_COLS para garantir que tentamos pegar tudo que é possível
            # independentemente de ser Lemit ou Assertiva (pois o mapeamento resolve as diferenças)
            st.session_state.df_clean, st.session_state.missing_cols, _ = clean_and_filter_data(df_raw, essential_cols=FULL_EXTRACTION_COLS)

            if st.session_state.df_clean.empty:
                st.warning("Atenção: Após a limpeza e filtragem, nenhum dado restou. Verifique os filtros aplicados e o mapeamento das colunas.")
                return

            st.dataframe(st.session_state.df_clean.head(50))
            st.info(f"Linhas finais: {len(st.session_state.df_clean)}")
            if st.session_state.missing_cols:
                st.warning(f"Colunas essenciais ausentes: {', '.join(st.session_state.missing_cols)}")

            df_export = st.session_state.df_clean.drop(columns=['Distancia'], errors='ignore')
            # Store export dataframe in session_state so download buttons and generators
            # can consistently access the same buffer/DF instance.
            st.session_state.df_export = df_export

            st.subheader("Opções de Exportação")
            if "filename" not in st.session_state:
                st.session_state.filename = f"relatorio_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            filename_input = st.text_input("Nome do arquivo (sem extensão)", value=st.session_state.filename, key="filename_input_key")
            st.session_state.filename = filename_input

            pdf_title_input = st.text_input("Título do PDF", value="Empresários CG", key="pdf_title_input_key")
            st.session_state.pdf_title = pdf_title_input

            current_date = datetime.now().strftime('%d-%m-%Y')
            final_output_filename = f"{st.session_state.filename}_{current_date}"

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Gerar e Baixar PDF"):
                    with st.spinner("Gerando PDF..."):
                        pdf_buffer = create_pdf_robust(st.session_state.df_export, title=st.session_state.pdf_title)
                        if pdf_buffer:
                            st.session_state.pdf_buffer = pdf_buffer
                            st.session_state.pdf_filename = final_output_filename + ".pdf"
                        else:
                            st.error("Falha ao gerar o PDF.")
            
            with col2:
                if st.button("Gerar e Baixar Excel (XLSX)"):
                    with st.spinner("Gerando Excel..."):
                        st.session_state.excel_buffer = generate_excel_buffer(st.session_state.df_export)
                        st.session_state.excel_filename = final_output_filename + ".xlsx"

            if 'pdf_buffer' in st.session_state and st.session_state.pdf_buffer:
                st.download_button(
                    label="Baixar PDF Gerado",
                    data=st.session_state.pdf_buffer,
                    file_name=st.session_state.pdf_filename,
                    mime="application/pdf",
                    key='download_pdf_higienizacao'
                )
                # Limpa o buffer após o botão ser exibido
                # st.session_state.pdf_buffer = None 

            if 'excel_buffer' in st.session_state and st.session_state.excel_buffer:
                st.download_button(
                    label="Baixar XLSX Gerado",
                    data=st.session_state.excel_buffer,
                    file_name=st.session_state.excel_filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key='download_excel_higienizacao'
                )
                # Limpa o buffer após o botão ser exibido
                # st.session_state.excel_buffer = None

def aba_divisor_listas():
    st.header("Divisor de Listas de Leads - Automoveis")
    st.info("Faça o upload de um arquivo com campos de 'Nome' e 'Celular'. Não é obrigatório ser exatamente os nomes.")
    uploaded_file = st.file_uploader("Faça upload do arquivo XLSX com os leads", type=["xlsx"], key="divisor_uploader")
    
    if uploaded_file:
        # Load raw data immediately after upload to get columns for mapping
        df_raw_leads, _, err = load_data(uploaded_file)
        if err:
            st.error(err)
            return

       # st.write("### Automação de Lista - Pessoas (Agendor)") # Título redundante se já está na aba
    


        col1_upload, col2_upload = st.columns(2)

        st.subheader("Opções de Filtragem e Distribuição")
        
        col1, col2 = st.columns(2)

        with col1:
            # Filtro por data de início
            start_date = st.date_input("Data de Início da Distribuição", value=date.today(), help="Selecione a data a partir da qual a distribuição de leads começará.")

        with col2:
            # Filtro por equipe/supervisor (dinâmico via JSON)
            equipes_json = carregar_equipes()
            all_teams = [e["nome"] for e in equipes_json]
            selected_teams = st.multiselect(
                "Filtrar por Equipe/Supervisor", 
                options=all_teams, 
                default=all_teams,
                help="Selecione as equipes cujos consultores devem receber leads. Se nenhuma for selecionada, todos os consultores serão considerados inicialmente.",
                key="divisor_filter_teams"
            )

        # Filtrar consultores a serem excluídos (mantido abaixo para melhor visualização de muitas opções)
        consultants_pool = []
        consultores_json = carregar_consultores()
        consultores_nomes = [c["consultor"] for c in consultores_json]
        if selected_teams:
            for team in selected_teams:
                for equipe in equipes_json:
                    if equipe["nome"] == team:
                        consultants_pool.extend(equipe["consultores"])
            consultants_pool = sorted(list(set(consultants_pool)))
        else:
            consultants_pool = sorted(consultores_nomes)

        excluded_consultants = st.multiselect(
            "Excluir Consultores Específicos", 
            options=consultants_pool,
            help="Selecione os consultores que NÃO devem receber leads nesta distribuição.",
            key="divisor_exclude_consultants"
        )

        leads_per_consultant = st.number_input("Quantidade de leads por consultor", min_value=1, value=50, help="Defina quantos leads cada consultor receberá por vez.")

        st.subheader("Mapeamento de Colunas de Entrada")
        st.info("O sistema tentará mapear as colunas 'NOME' e 'Whats' automaticamente. Verifique e ajuste se necessário.")

        df_leads_cols = df_raw_leads.columns.tolist()
        expected_cols_divisor = ["NOME", "Whats", "CEL"]
        
        # Sugestões de nomes de colunas para pré-seleção automática
        SUGGESTED_COLUMN_NAMES = {
            "NOME": ["NOME", "Nome Completo", "Cliente", "Razao Social", "Empresa", "NOME/RAZAO_SOCIAL", "Socio1Nome", "Nome", "Razao"],
            "Whats": ["Whats", "WhatsApp", "Telefone", "Celular", "Contato", "CELULAR1", "SOCIO1Celular1", "Socio1Celular1"],
            "CEL": ["CEL", "Celular", "Telefone", "Whats", "WhatsApp", "CELULAR2", "SOCIO1Celular2", "Socio1Celular2"]
        }

        user_col_mapping = {}
        # Mapeia as colunas do arquivo para minúsculas para busca case-insensitive
        df_cols_lower_map = {c.lower(): c for c in reversed(df_leads_cols)}

        for col in expected_cols_divisor:
            default_selection = ''
            
            # A lista de busca prioriza o nome exato da coluna esperada, depois as sugestões
            search_list = [col] + SUGGESTED_COLUMN_NAMES.get(col, [])

            # Usa a função de matching robusto para encontrar a melhor coluna
            default_selection = best_match_column(df_leads_cols, search_list)
            
            # Determina o índice da opção pré-selecionada para o selectbox
            try:
                # Adiciona 1 porque a lista de opções do selectbox começa com um item vazio ''
                default_index = df_leads_cols.index(default_selection) + 1 if default_selection else 0
            except ValueError:
                default_index = 0

            selected_col = st.selectbox(
                f"Coluna para '{col}'",
                options=[''] + df_leads_cols,
                index=default_index,
                key=f"map_divisor_{col}"
            )
            user_col_mapping[col] = selected_col
        
        

        if st.button("Processar e Gerar Listas"):
            with st.spinner("Processando... Por favor, aguarde."):
                try:
                    # Validate NOME mapping before proceeding
                    if not user_col_mapping["NOME"]:
                        st.warning("A coluna 'NOME' é obrigatória para a distribuição de leads.")
                        return

                    # Apply mapping and rename DataFrame
                    df_leads_mapped = df_raw_leads.copy()
                    for expected, actual in user_col_mapping.items():
                        if actual: # Only process if a column was selected
                            if actual in df_leads_mapped.columns:
                                df_leads_mapped.rename(columns={actual: expected}, inplace=True)
                            else:
                                st.warning(f"A coluna '{actual}' selecionada para '{expected}' não foi encontrada no arquivo. Verifique o mapeamento.")
                                return

                    # Validate if NOME column exists after mapping
                    if "NOME" not in df_leads_mapped.columns:
                        st.warning("A coluna 'NOME' é obrigatória para a distribuição de leads e não foi mapeada corretamente.")
                        return

                    # Limpa e filtra pelo número de WhatsApp
                    if "Whats" in df_leads_mapped.columns:
                        initial_rows = len(df_leads_mapped)
                        df_leads_mapped["Whats"] = df_leads_mapped["Whats"].apply(clean_phone_number)
                        df_leads_mapped.dropna(subset=["Whats"], inplace=True)
                        final_rows = len(df_leads_mapped)
                        removed = initial_rows - final_rows
                        if removed > 0:
                            st.info(f"{removed} linhas foram removidas por não conterem um número de WhatsApp válido.")
                    else:
                        st.warning("A coluna 'Whats' não foi mapeada. Nenhuma filtragem por WhatsApp foi aplicada.")

                    if df_leads_mapped.empty:
                        st.warning("Após a filtragem, não restaram leads para distribuir.")
                        return

                    # Determine effective consultants based on filters
                    effective_consultores = []
                    if selected_teams:
                        for team in selected_teams:
                            for equipe in equipes_json:
                                if equipe["nome"] == team:
                                    effective_consultores.extend(equipe["consultores"])
                        effective_consultores = list(set(effective_consultores))
                    else:
                        effective_consultores = list(consultores_nomes)

                    if excluded_consultants:
                        effective_consultores = [c for c in effective_consultores if c not in excluded_consultants]
                    effective_consultores.sort()

                    if not effective_consultores:
                        st.warning("Nenhum consultor selecionado após a aplicação dos filtros. Ajuste suas seleções.")
                        return

                    # Clean CEL column (now in df_leads_mapped)
                    if "CEL" in df_leads_mapped.columns:
                        df_leads_mapped["CEL"] = pd.to_numeric(df_leads_mapped["CEL"], errors='coerce')
                        df_leads_mapped["CEL"] = df_leads_mapped["CEL"].astype('Int64').astype(str).replace('<NA>', '')
                    
                    zip_buffer = io.BytesIO()
                    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                        leads_processados = 0
                        data_atual = start_date
                        total_leads = len(df_leads_mapped)
                        arquivos_gerados = 0

                        while leads_processados < total_leads:
                            for consultor in effective_consultores:
                                if leads_processados >= total_leads: 
                                    break

                                inicio_lote = leads_processados
                                fim_lote = leads_processados + leads_per_consultant
                                st.info(f"Processando leads de {inicio_lote} a {fim_lote} para o consultor {consultor}")
                                df_lote = df_leads_mapped.iloc[inicio_lote:fim_lote].copy()

                                # Convert numeric columns to string
                                for col in df_lote.columns:
                                    if pd.api.types.is_numeric_dtype(df_lote[col]):
                                        df_lote[col] = df_lote[col].astype('Int64').astype(str).replace('<NA>', '')

                                # Define and ensure checkbox columns
                                cols_to_center = ["1º Contato", "2º Contato", "3º Contato", "Atend. Lig.(S/N)", "Visita Marc.(S/N)"]
                                cols_single_checkbox = ["1º Contato", "2º Contato", "3º Contato"]
                                cols_double_checkbox = ["Atend. Lig.(S/N)", "Visita Marc.(S/N)"]

                                for col in cols_single_checkbox:
                                    if col not in df_lote.columns:
                                        df_lote[col] = "☐"
                                    else:
                                        df_lote[col] = "☐"
                                
                                for col in cols_double_checkbox:
                                    if col not in df_lote.columns:
                                        df_lote[col] = "☐   ☐"
                                    else:
                                        df_lote[col] = "☐   ☐"
                                
                                if not df_lote.empty:
                                    excel_buffer = generate_excel_buffer(df_lote)
                                    
                                    primeiro_nome = consultor.split(' ')[0]
                                    data_formatada_nome = data_atual.strftime('%d_%m_%Y')
                                    nome_arquivo_base = f"LEADS_AUTOMOVEIS_{primeiro_nome.upper()}_{data_formatada_nome}"
                                    
                                    # Buscar equipe do consultor via JSON
                                    nome_equipe = "Outros"
                                    for equipe in equipes_json:
                                        if consultor in equipe["consultores"]:
                                            nome_equipe = equipe["nome"]
                                            break
                                    zip_file.writestr(f"{nome_equipe}/{nome_arquivo_base}.xlsx", excel_buffer.getvalue())

                                    pdf_title = f"Leads Automoveis - {primeiro_nome} {data_atual.strftime('%d/%m')}"
                                    pdf_buffer = create_pdf_robust(df_lote, title=pdf_title, cols_to_center=cols_to_center, cols_single_checkbox=cols_single_checkbox, cols_double_checkbox=cols_double_checkbox)
                                    
                                    if pdf_buffer:
                                        zip_file.writestr(f"{nome_equipe}/{nome_arquivo_base}.pdf", pdf_buffer.getvalue())
                                    
                                    leads_processados += len(df_lote)
                                    arquivos_gerados += 1

                            data_atual = proximo_dia_util(data_atual)
                    
                    st.success(f"Processo concluído! {arquivos_gerados} pares de listas (Excel e PDF) foram gerados.")

                    

                    zip_filename = f"Listas_Consultores_{datetime.now().strftime('%d-%m-%Y')}.zip"
                    
                    zip_filename = f"Listas_Consultores_{datetime.now().strftime('%d-%m-%Y')}.zip"
                    st.download_button(
                        label="Baixar Todas as Listas (ZIP)",
                        data=zip_buffer.getvalue(),
                        file_name=zip_filename,
                        mime="application/zip"
                    )

                except Exception as e:
                    logging.exception("Erro ao processar divisor de listas")
                    st.error(f"Ocorreu um erro durante o processamento: {e}")

def aba_gerador_negocios_robos():
    st.header("Gerador de Negócios para Robôs")

    # --- Lógica de Detecção de Fonte de Dados ---
    source_mode = st.session_state.get('source_for_negocios', 'upload') # Default to upload

    # Se o handoff foi ativado, muda o modo
    if st.session_state.get('handoff_active', False):
        source_mode = 'handoff'

    # --- Renderização da UI baseada no modo ---

    # MODO 1: Handoff a partir da aba Pessoas Agendor
    if source_mode == 'handoff':
        st.info("Gerando negócios a partir dos leads recém-criados na aba anterior.")
        
        generated_files = st.session_state.get('generated_pessoas_files', {})
        if not generated_files:
            st.error("Não foram encontrados dados de leads para processar. Por favor, gere os arquivos na aba 'Automação Pessoas Agendor' primeiro.")
            st.session_state.handoff_active = False # Limpa a flag
            return

        st.subheader(f"{len(generated_files)} arquivo(s) de 'Pessoas' pronto(s) para processar.")

        # --- Configurações de Negócio (Interface Simplificada) ---
        st.subheader("Configurações para Geração de Negócios")
        col1, col2 = st.columns(2)
        with col1:
            negocios_por_consultor = st.number_input("Número de negócios por consultor (por arquivo)", min_value=1, value=20, key="negocios_handoff")
        with col2:
            start_date_negocios = st.date_input("Data de Início para Negócios", value=date.today(), key="date_handoff")

        col3, col4 = st.columns(2)
        with col3:
            nicho_principal = st.text_input("Nicho Principal (ex: AUTO, MED, EMPR)", value="AUTO", key="nicho_handoff")
        with col4:
            sufixo_localidade = st.text_input("Sufixo de Localidade (opcional, ex: CG, MS)", value="", key="sufixo_handoff")
        
        if st.button("Gerar Arquivos de Negócios", key="btn_gerar_handoff"):
            # A lógica de geração usará `generated_files` do session_state
            processar_e_gerar_negocios(negocios_por_consultor, start_date_negocios, nicho_principal, sufixo_localidade, source_data=generated_files)
            # Limpa a flag após o processo
            st.session_state.handoff_active = False
            st.session_state.source_for_negocios = 'upload' # Reseta para o padrão

    # MODO 2: Upload de arquivo cru
    else:
        st.info("Faça o upload de um arquivo de leads (XLSX ou CSV) para iniciar a geração de negócios.")
        uploaded_file = st.file_uploader("Selecione um arquivo de leads", type=["xlsx", "csv"], key="negocios_uploader")

        if uploaded_file:
            # Reset handoff state if a new file is uploaded in raw mode
            if st.session_state.get('source_for_negocios') == 'handoff':
                st.session_state.handoff_active = False
                st.session_state.source_for_negocios = 'upload'

            df_raw_leads, _, err = load_data(uploaded_file)
            if err:
                st.error(err)
                return

            st.dataframe(df_raw_leads.head())

            # --- Mapeamento de Colunas ---
            st.subheader("1. Mapeamento de Colunas")
            df_cols = df_raw_leads.columns.tolist()

            # Heurística de pré-seleção: tenta detectar automaticamente as colunas de Nome e WhatsApp
            df_cols_lower_map = {c.lower(): c for c in df_cols}
            SUGGESTED_NAME_COLS = ["nome", "nome completo", "name", "razao social", "razão social", "empresa"]
            SUGGESTED_WHATS_COLS = ["whats", "whatsapp", "telefone", "celular", "contato"]

            # Usa matching robusto para tentar encontrar as colunas de nome e whatsapp
            default_nome = best_match_column(df_cols, SUGGESTED_NAME_COLS)
            default_whats = best_match_column(df_cols, SUGGESTED_WHATS_COLS)

            # Se houver uma coluna explicitamente contendo 'whats' ou 'whatsapp', prefira-a
            explicit_whats = None
            for c in df_cols:
                cl = c.lower()
                if 'whats' in cl or 'whatsapp' in cl:
                    explicit_whats = c
                    break
            if explicit_whats:
                # Só sobrescreve se a detecção atual não for explícita
                if not default_whats or ('whats' not in default_whats.lower() and 'whatsapp' not in default_whats.lower()):
                    default_whats = explicit_whats

            # Determinar índices padrão para os selectboxes
            try:
                default_nome_index = df_cols.index(default_nome) + 1 if default_nome else 0
            except ValueError:
                default_nome_index = 0
            try:
                default_whats_index = df_cols.index(default_whats) + 1 if default_whats else 0
            except ValueError:
                default_whats_index = 0

            map_col1, map_col2 = st.columns(2)
            with map_col1:
                nome_col = st.selectbox("Coluna com o NOME do lead", [''] + df_cols, index=default_nome_index, key="map_nome_negocios")
            with map_col2:
                whats_col = st.selectbox("Coluna com o WHATSAPP do lead", [''] + df_cols, index=default_whats_index, key="map_whats_negocios")

            # --- Distribuição de Consultores ---
            st.subheader("2. Distribuição de Consultores")
            dist_mode = st.radio(
                "Como distribuir os leads?",
                ["Distribuir para Todos", "Distribuir para Todos, EXCETO...", "Distribuir APENAS para..."],
                key="dist_mode_negocios"
            )

            consultores_json = carregar_consultores()
            consultores_nomes = sorted([c["consultor"] for c in consultores_json])
            effective_consultores = []
            if dist_mode == "Distribuir para Todos":
                effective_consultores = consultores_nomes
                st.success(f"{len(effective_consultores)} consultores receberão os leads.")
            elif dist_mode == "Distribuir para Todos, EXCETO...":
                excluded = st.multiselect("Selecione os consultores a EXCLUIR:", options=consultores_nomes, key="exclude_negocios")
                effective_consultores = [c for c in consultores_nomes if c not in excluded]
                st.success(f"{len(effective_consultores)} consultores receberão os leads.")
            elif dist_mode == "Distribuir APENAS para...":
                included = st.multiselect("Selecione os consultores a INCLUIR:", options=consultores_nomes, key="include_negocios")
                effective_consultores = included
                st.success(f"{len(effective_consultores)} consultores receberão os leads.")

            # --- Configurações de Negócio ---
            st.subheader("3. Configurações para Geração de Negócios")
            col1, col2 = st.columns(2)
            with col1:
                negocios_por_consultor_upload = st.number_input("Número de negócios por consultor (por arquivo)", min_value=1, value=20, key="negocios_upload")
            with col2:
                start_date_negocios_upload = st.date_input("Data de Início para Negócios", value=date.today(), key="date_upload")

            col3, col4 = st.columns(2)
            with col3:
                nicho_principal_upload = st.text_input("Nicho Principal (ex: AUTO, MED, EMPR)", value="AUTO", key="nicho_upload")
            with col4:
                sufixo_localidade_upload = st.text_input("Sufixo de Localidade (opcional, ex: CG, MS)", value="", key="sufixo_upload")

            generate_button_disabled = not nome_col or not whats_col
            if generate_button_disabled:
                st.warning("Mapeamento de colunas 'Nome' e 'WhatsApp' é obrigatório para gerar os arquivos.")

            if st.button("Gerar Arquivos de Negócios", key="btn_gerar_upload", disabled=generate_button_disabled):
                if not effective_consultores:
                    st.error("Nenhum consultor foi selecionado para a distribuição. Verifique os filtros.")
                    return
                
                # Preparar o DataFrame
                df_renamed = df_raw_leads.rename(columns={nome_col: "Nome", whats_col: "WhatsApp"})
                df_renamed["WhatsApp"] = df_renamed["WhatsApp"].apply(clean_phone_number)
                df_renamed.dropna(subset=["WhatsApp"], inplace=True)

                if df_renamed.empty:
                    st.warning("Após a filtragem, não restaram leads para distribuir.")
                    return

                processar_e_gerar_negocios(
                    negocios_por_consultor_upload,
                    start_date_negocios_upload,
                    nicho_principal_upload,
                    sufixo_localidade_upload,
                    df_raw=df_raw_leads, # Pass the raw DataFrame
                    col_mapping={"Nome": nome_col, "WhatsApp": whats_col}, # Pass the column mapping
                    effective_consultores=effective_consultores # Pass the effective consultants
                )


def processar_e_gerar_negocios(negocios_por_consultor, start_date_negocios, nicho_principal, sufixo_localidade, source_data=None, df_raw=None, col_mapping=None, effective_consultores=None):
    """Função unificada para gerar arquivos de negócios."""
    with st.spinner("Gerando arquivos de Negócios... Por favor, aguarde."):
        all_generated_files = {}
        processing_logs = []  # Log list for UI feedback

        if source_data is not None: # Modo Handoff ou Upload pré-processado
            for file_name, file_data in source_data.items():
                try:
                    df_pessoas = pd.read_excel(io.BytesIO(file_data))
                    # Extrair o nome do consultor do nome do arquivo de pessoas
                    file_name_only = os.path.basename(file_name)
                    file_name_parts = file_name_only.replace(".xlsx", "").split('_')
                    consultor_nome_arquivo = ""
                    if len(file_name_parts) >= 4:
                        consultor_nome_arquivo = file_name_parts[3] # Pega o nome do consultor
                    
                    if not consultor_nome_arquivo:
                        st.warning(f"Não foi possível extrair o nome do consultor do arquivo: {file_name_only}. Pulando este arquivo.")
                        continue

                    # Colunas da planilha de Negócios
                    colunas_negocios = [
                        "Título do negócio", "Empresa relacionada", "Pessoa relacionada",
                        "Usuário responsável", "Data de início", "Data de conclusão",
                        "Valor Total", "Funil", "Etapa", "Status", "Motivo de perda",
                        "Descrição do motivo de perda", "Ranking", "Descrição", "Produtos e Serviços"
                    ]

                    leads_do_consultor = df_pessoas.copy()
                    
                    # Garantir que as colunas essenciais existam
                    required_cols_pessoas = ["Nome", "Usuário responsável", "WhatsApp"]
                    if not all(col in leads_do_consultor.columns for col in required_cols_pessoas):
                        st.warning(f"Arquivo {file_name_only} não contém todas as colunas essenciais (Nome, Usuário responsável, WhatsApp). Pulando este arquivo.")
                        continue


                    # Limpar e formatar WhatsApp para uso em Data de Conclusão - USANDO PRESERVE_FULL para evitar cortes incorretos
                    leads_do_consultor["WhatsApp_Clean"] = leads_do_consultor["WhatsApp"].apply(lambda x: clean_phone_number(x, preserve_full=True))
                    leads_do_consultor["WhatsApp_Clean"] = leads_do_consultor["WhatsApp_Clean"].apply(lambda x: str(x) if pd.notna(x) else "")

                    num_leads_consultor = len(leads_do_consultor)
                    leads_processados_consultor = 0
                    current_date = start_date_negocios
                    file_counter = 1

                    while leads_processados_consultor < num_leads_consultor:
                        inicio_lote = leads_processados_consultor
                        fim_lote = min(leads_processados_consultor + negocios_por_consultor, num_leads_consultor)
                        df_lote_negocios = leads_do_consultor.iloc[inicio_lote:fim_lote].copy()

                        if not df_lote_negocios.empty:
                            dados_negocios = []
                            for _, row_lead in df_lote_negocios.iterrows():
                                nome_pessoa = row_lead.get("Nome", "")
                                usuario_responsavel = row_lead.get("Usuário responsável", "")
                                whatsapp_lead = row_lead.get("WhatsApp_Clean", "")
                                
                                # Lógica inteligente de DDI (Centralizada)
                                whatsapp_lead_full, status_phone = format_phone_for_whatsapp_business(whatsapp_lead)
                                
                                if status_phone == "VAZIO":
                                    processing_logs.append(f"❌ [Handoff] {nome_pessoa}: Sem WhatsApp válido. Campo Data de Conclusão ficará vazio.")
                                elif status_phone == "INVÁLIDO (Curto)":
                                    processing_logs.append(f"⚠️ [Handoff] {nome_pessoa}: Número curto detectado ({whatsapp_lead}).")

                                # Formatar Título do negócio usando a data do arquivo (current_date)
                                mes_ano = current_date.strftime('%m/%y')
                                nicho_formatado_titulo = nicho_principal.upper()
                                if sufixo_localidade:
                                    nicho_formatado_titulo += f" {sufixo_localidade.upper()}"
                                
                                titulo_negocio = f"{mes_ano} - RB - {nicho_formatado_titulo} - {nome_pessoa}/ESPs"

                                linha_negocio = {
                                    "Título do negócio": titulo_negocio,
                                    "Empresa relacionada": "", # Deixar em branco
                                    "Pessoa relacionada": nome_pessoa,
                                    "Usuário responsável": usuario_responsavel,
                                    "Data de início": current_date.strftime('%d/%m/%Y'),
                                    "Data de conclusão": whatsapp_lead_full, # WhatsApp com DDI +55
                                    "Valor Total": "", # Deixar em branco
                                    "Funil": "Funil de Vendas",
                                    "Etapa": "Prospecção",
                                    "Status": "Em andamento",
                                    "Motivo de perda": "", # Deixar em branco
                                    "Descrição do motivo de perda": "", # Deixar em branco
                                    "Ranking": "", # Deixar em branco
                                    "Descrição": "", # Deixar em branco
                                    "Produtos e Serviços": "" # Deixar em branco
                                }
                                dados_negocios.append(linha_negocio)
                            
                            df_final_negocios = pd.DataFrame(dados_negocios, columns=colunas_negocios)

                            output_excel_negocios = generate_excel_buffer(df_final_negocios)

                            # Nome do arquivo de negócios
                            nome_arquivo_negocios = f"NEGOCIOS_{consultor_nome_arquivo.upper()}_{nicho_principal.upper()}"
                            if sufixo_localidade:
                                nome_arquivo_negocios += f"_{sufixo_localidade.upper()}"
                            nome_arquivo_negocios += f"_{current_date.strftime('%d-%m-%Y')}.xlsx"
                            
                            all_generated_files[nome_arquivo_negocios] = output_excel_negocios.getvalue()

                            leads_processados_consultor += len(df_lote_negocios)
                            current_date = proximo_dia_util(current_date) # Avança a data para o próximo arquivo
                            file_counter += 1

                except Exception as e:
                    logging.exception(f"Erro ao processar o arquivo {file_name}")
                    st.error(f"Erro ao processar o arquivo {file_name}: {e}")
                    continue
        
        else: # Modo de upload de arquivo cru (df_raw, col_mapping, effective_consultores)
            if df_raw is None or col_mapping is None or effective_consultores is None:
                st.error("Erro interno: Parâmetros ausentes para o modo de arquivo cru.")
                return

            # Preparar o DataFrame
            df_renamed = df_raw.rename(columns={col_mapping["Nome"]: "Nome", col_mapping["WhatsApp"]: "WhatsApp"})
            # Usar preserve_full=True para não cortar dígitos inadvertidamente
            df_renamed["WhatsApp"] = df_renamed["WhatsApp"].apply(lambda x: clean_phone_number(x, preserve_full=True))
            
            # Count dropped rows for logging
            initial_count = len(df_renamed)
            df_renamed.dropna(subset=["WhatsApp"], inplace=True)
            dropped_count = initial_count - len(df_renamed)
            if dropped_count > 0:
                processing_logs.append(f"⚠️ [Upload] {dropped_count} leads removidos pois a coluna WhatsApp estava vazia ou inválida após limpeza.")

            if df_renamed.empty:
                st.warning("Após a filtragem, não restaram leads para distribuir.")
                return

            # Distribuir leads entre consultores
            leads_por_consultor_dist = np.array_split(df_renamed, len(effective_consultores))
            
            # Colunas da planilha de Negócios
            colunas_negocios = [
                "Título do negócio", "Empresa relacionada", "Pessoa relacionada",
                "Usuário responsável", "Data de início", "Data de conclusão",
                "Valor Total", "Funil", "Etapa", "Status", "Motivo de perda",
                "Descrição do motivo de perda", "Ranking", "Descrição", "Produtos e Serviços"
            ]

            leads_processados = 0
            current_date = start_date_negocios
            file_counter = 1

            for i, consultor in enumerate(effective_consultores):
                # Se houver mais consultores do que lotes (ex: 3 consultores, 2 leads), 
                # os ultimos nao recebem nada. O array_split garante divisao justa.
                if i >= len(leads_por_consultor_dist):
                    break

                df_consultor = leads_por_consultor_dist[i].copy()
                total_leads = len(df_consultor)
                leads_processados = 0
                
                # Colunas da planilha de Negócios (com nova coluna de status)
                colunas_negocios = [
                    "Título do negócio", "Empresa relacionada", "Pessoa relacionada",
                    "Usuário responsável", "Data de início", "Data de conclusão",
                    "Valor Total", "Funil", "Etapa", "Status", "Motivo de perda",
                    "Descrição do motivo de perda", "Ranking", "Descrição", "Produtos e Serviços",
                    "Status Telefone"
                ]

                # Reinicia data para cada consultor ? (Baseado na lógica anterior sim)
                current_date = start_date_negocios
                file_counter = 1

                while leads_processados < total_leads:
                    inicio_lote = leads_processados
                    fim_lote = min(leads_processados + negocios_por_consultor, total_leads)
                    df_lote_negocios = df_consultor.iloc[inicio_lote:fim_lote].copy()
                    
                    if not df_lote_negocios.empty:
                        dados_negocios = []
                        for _, row_lead in df_lote_negocios.iterrows():
                            nome_pessoa = row_lead.get("Nome", "")
                            usuario_responsavel = consultor.lower().replace(' ', '.')
                            whatsapp_lead = row_lead.get("WhatsApp", "")
                            
                            # Clean once
                            cleaned = clean_phone_number(whatsapp_lead, preserve_full=True)
                            whatsapp_lead_clean = str(cleaned) if pd.notna(cleaned) else ""
                            
                            # Lógica inteligente de DDI para Upload Cru + Flagging (Centralizada)
                            whatsapp_lead_full, status_telefone = format_phone_for_whatsapp_business(whatsapp_lead_clean)

                            if status_telefone == "VAZIO":
                                processing_logs.append(f"❌ [Upload] {nome_pessoa}: WhatsApp vazio após limpeza.")
                            elif status_telefone == "INVÁLIDO (Curto)":
                                processing_logs.append(f"⚠️ [Upload] {nome_pessoa}: Número curto ({whatsapp_lead_clean}).")


                            # Use the file's current_date for month/year in title
                            mes_ano = current_date.strftime('%m/%y')
                            nicho_formatado_titulo = nicho_principal.upper()
                            if sufixo_localidade:
                                nicho_formatado_titulo += f" {sufixo_localidade.upper()}"
                            titulo_negocio = f"{mes_ano} - RB - {nicho_formatado_titulo} - {nome_pessoa}/ESPs"

                            linha_negocio = {
                                "Título do negócio": titulo_negocio,
                                "Empresa relacionada": "",
                                "Pessoa relacionada": nome_pessoa,
                                "Usuário responsável": usuario_responsavel,
                                "Data de início": current_date.strftime('%d/%m/%Y'),
                                "Data de conclusão": whatsapp_lead_full,
                                "Valor Total": "",
                                "Funil": "Funil de Vendas",
                                "Etapa": "Prospecção",
                                "Status": "Em andamento",
                                "Motivo de perda": "",
                                "Descrição do motivo de perda": "",
                                "Ranking": "",
                                "Descrição": "",
                                "Produtos e Serviços": "",
                                "Status Telefone": status_telefone # New Column
                            }
                            dados_negocios.append(linha_negocio)
                        
                        df_final_negocios = pd.DataFrame(dados_negocios, columns=colunas_negocios)

                        output_excel_negocios = generate_excel_buffer(df_final_negocios)

                        primeiro_nome_consultor = consultor.split(' ')[0].upper()
                        nome_arquivo_negocios = f"NEGOCIOS_{primeiro_nome_consultor}_{nicho_principal.upper()}"
                        if sufixo_localidade:
                            nome_arquivo_negocios += f"_{sufixo_localidade.upper()}"
                        nome_arquivo_negocios += f"_{current_date.strftime('%d-%m-%Y')}.xlsx"

                        all_generated_files[nome_arquivo_negocios] = output_excel_negocios.getvalue()

                        leads_processados += len(df_lote_negocios)
                        current_date = proximo_dia_util(current_date)
                        file_counter += 1
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                for file_name_in_zip, file_data in all_generated_files.items():
                    zip_file.writestr(file_name_in_zip, file_data)
            # Nome do zip: se só um consultor, usa o nome dele, senão usa "varios"
            if len(effective_consultores) == 1:
                # Buscar usuário do consultor
                usuario = None
                try:
                    with open(CONSULTORES_FILE, "r", encoding="utf-8") as f:
                        consultores_data = json.load(f)
                        for c in consultores_data:
                            if c["consultor"].strip().lower() == effective_consultores[0].strip().lower():
                                usuario = c["usuario"].replace(" ", "_").lower()
                                break
                except Exception:
                    usuario = effective_consultores[0].replace(" ", "_").lower()
                zip_filename = f"Negocios_Robos_{usuario}.zip"
            else:
                zip_filename = f"Negocios_Robos_varios.zip"
            st.download_button(
                label="Baixar Todos os Arquivos de Negócios (ZIP)",
                data=zip_buffer.getvalue(),
                file_name=zip_filename,
                mime="application/zip",
                key="download_negocios_zip"
            )
            st.success(f"Processo concluído! {len(all_generated_files)} arquivos de Negócios gerados.")
            # Reset session state flags after successful generation
            st.session_state.handoff_active = False
            st.session_state.source_for_negocios = 'upload'

            # Exibir Logs de Processamento
            if processing_logs:
                with st.expander("Logs de Processamento (Avisos e Erros)", expanded=True):
                    for log_msg in processing_logs:
                        if "❌" in log_msg:
                            st.error(log_msg)
                        else:
                            st.warning(log_msg)
                    st.caption("Verifique se os números marcados como curtos ou inválidos estão corretos na planilha original.")
                
        if not all_generated_files:
            st.warning("Nenhum arquivo de Negócios foi gerado. Verifique os arquivos de entrada e as configurações.")



def aba_automacao_pessoas_agendor():
    st.header("Automação Pessoas Agendor")
    # st.write("### Automação de Lista - Pessoas (Agendor)") 

    st.info("Faça o upload de um arquivo de lista para iniciar a geração de pessoas. Obrigatório que o arquivo contenha as colunas 'NOME' e 'Whats'.")

    uploaded_file = st.file_uploader("Faça upload do arquivo XLSX com os leads", type=["xlsx"], key="geracao_pessoas_uploader")

    if uploaded_file:
        df_raw_leads, _, err = load_data(uploaded_file)
        if err:
            st.error(err)
            return
            
        df_leads_cols = df_raw_leads.columns.tolist() # Ensure this is defined for later use

        # Guarda o arquivo gerado para uso posterior (ex: Handoff)
        if 'generated_pessoas_files' not in st.session_state:
            st.session_state.generated_pessoas_files = {}

        st.subheader("Opções de Filtragem e Distribuição")

        dist_mode = st.radio(
            "Como distribuir os leads?",
            ["Distribuir para Todos", "Distribuir para Todos, EXCETO...", "Distribuir APENAS para..."],
            key="dist_mode_agendor"
        )

        consultores_json = carregar_consultores()
        consultores_nomes = sorted([c["consultor"] for c in consultores_json])
        effective_consultores = []
        if dist_mode == "Distribuir para Todos":
            effective_consultores = consultores_nomes
            st.success(f"{len(effective_consultores)} consultores receberão os leads.")
        elif dist_mode == "Distribuir para Todos, EXCETO...":
            excluded = st.multiselect("Selecione os consultores a EXCLUIR:", options=consultores_nomes, key="exclude_agendor")
            effective_consultores = [c for c in consultores_nomes if c not in excluded]
            st.success(f"{len(effective_consultores)} consultores receberão os leads.")
        elif dist_mode == "Distribuir APENAS para...":
            included = st.multiselect("Selecione os consultores a INCLUIR:", options=consultores_nomes, key="include_agendor")
            effective_consultores = included
            st.success(f"{len(effective_consultores)} consultores receberão os leads.")

        st.subheader("Configurações Adicionais para Agendor")
        default_cargo = st.text_input("Cargo Padrão", value="Lead Automovel", help="Cargo a ser atribuído aos leads no Agendor.")
        
        # --- Helper para criar Toggle estilo "Segmented Control" (Pílula) ---
        def create_toggle(label, options, default, key):
            try:
                # Tenta usar st.segmented_control (Novo no Streamlit 1.39+)
                # Ele retorna None se nada for selecionado, mas com 'default' garantimos o valor
                sel = st.segmented_control(label, options, default=default, key=key)
                return sel if sel else default # Garante retorno
            except AttributeError:
                # Fallback para st.radio antigo se a versão for anterior
                return st.radio(label, options, horizontal=True, index=options.index(default), key=key)

        # Toggle for Descrição - Estilo Pílula Horizontal
        st.write("**Configuração da Descrição**")
        desc_mode = create_toggle("Fonte da Descrição:", ["Valor Fixo", "Usar Coluna"], default="Valor Fixo", key="desc_mode_toggle")
        
        default_descricao = ""
        col_descricao = None
        
        if desc_mode == "Valor Fixo":
            default_descricao = st.text_area("Digite a Descrição Padrão", value="", help="Esta descrição será usada para todos os leads.")
        else:
            col_descricao = st.selectbox("Selecione a coluna de Descrição do arquivo:", options=[""] + df_leads_cols, key="col_descricao_select")

        # Toggle for UF - Estilo Pílula Horizontal
        st.write("**Configuração da UF (Estado)**")
        uf_mode = create_toggle("Fonte da UF:", ["Valor Fixo", "Usar Coluna"], default="Valor Fixo", key="uf_mode_toggle")
        
        default_uf = "MS"
        col_uf = None
        
        if uf_mode == "Valor Fixo":
            default_uf = st.text_input("Digite a UF Padrão", value="MS", max_chars=2, help="UF padrão para os leads.")
        else:
            col_uf = st.selectbox("Selecione a coluna de UF do arquivo:", options=[""] + df_leads_cols, key="col_uf_select")
            
        nicho_valor = st.text_input("Nicho (para nome do arquivo)", value="GERAL", help="Valor do nicho para o nome do arquivo de exportação (ex: AUTOMOVEIS, IMOVEIS).")

        st.subheader("Mapeamento de Colunas")
        st.info("Selecione as colunas do seu arquivo que correspondem aos campos esperados.")


        
        # Suggested column names for pre-selection
        SUGGESTED_COLUMN_NAMES_AGENDOR = {
            "NOME": ["NOME", "Nome Completo", "Socio1Nome", "Razao Social", "Razão Social", "Empresa", "NOME/RAZAO_SOCIAL"],
            "Whats": ["Whats", "WhatsApp", "Telefone", "Celular", "Contato", "CELULAR1", "CELULAR2", "SOCIO1Celular1"],
            "CEL": ["CEL", "Celular", "Telefone", "Whats", "WhatsApp", "CELULAR1", "CELULAR2", "SOCIO1Celular2"],
            "Rua": ["Rua", "Logradouro", "Endereco", "Endereço"],
            "Número": ["Numero", "Número", "Num"],
            "Bairro": ["Bairro"],
            "Cidade": ["Cidade"],
            "CEP": ["CEP", "Cep", "cep", "Codigo Postal", "Código Postal", "CodigoPostal"],
            "Razao Social": ["Razao Social", "Razão Social", "Razao", "RAZAO_SOCIAL"],
            "Fantasia": ["Fantasia", "Nome Fantasia", "NomeFantasia"],
            "Complemento": ["Complemento", "Complemento Endereco", "Comp"]
        }

        expected_cols_agendor = ["NOME", "Whats", "CEL", "Rua", "Número", "Bairro", "Cidade", "CEP", "Razao Social", "Fantasia", "Complemento"]
        user_col_mapping = {}
        # Mapeia as colunas do arquivo para minúsculas para busca case-insensitive
        df_cols_lower_map = {c.lower(): c for c in reversed(df_leads_cols)}

        for col in expected_cols_agendor:
            default_selection = ''
            
            # A lista de busca prioriza o nome exato da coluna esperada, depois as sugestões
            search_list = [col] + SUGGESTED_COLUMN_NAMES_AGENDOR.get(col, [])

            # Usa matching robusto (substring/similarity) para encontrar a melhor coluna
            default_selection = best_match_column(df_leads_cols, search_list)

            # Preferência explícita: se estivermos buscando pela coluna de Whats,
            # prefira qualquer coluna que contenha 'whats' ou 'whatsapp' no nome.
            if col.lower() == 'whats':
                explicit_whats = None
                for c in df_leads_cols:
                    cl = c.lower()
                    if 'whats' in cl or 'whatsapp' in cl:
                        explicit_whats = c
                        break
                if explicit_whats:
                    if not default_selection or ('whats' not in default_selection.lower() and 'whatsapp' not in default_selection.lower()):
                        default_selection = explicit_whats
            
            # Determina o índice da opção pré-selecionada para o selectbox
            try:
                # Adiciona 1 porque a lista de opções do selectbox começa com um item vazio ''
                default_index = df_leads_cols.index(default_selection) + 1 if default_selection else 0
            except ValueError:
                default_index = 0

            selected_col = st.selectbox(
                f"Coluna para '{col}'",
                options=[''] + df_leads_cols,
                index=default_index,
                key=f"map_agendor_{col}"
            )
            user_col_mapping[col] = selected_col

        # Lógica para input de leads por consultor e divisão forçada
        force_split = False
        if len(effective_consultores) == 1:
            force_split = st.checkbox("Forçar divisão em lotes mesmo com 1 consultor", value=False, key="force_split_single")
            leads_por_consultor = st.number_input("Número de leads por consultor", min_value=1, value=50, disabled=not force_split)
            if not force_split:
                st.info("Apenas 1 consultor selecionado — por padrão ele receberá todos os leads. Marque a opção acima para dividir em lotes.")
        else:
            leads_por_consultor = st.number_input("Número de leads por consultor", min_value=1, value=50)

        if st.button("Gerar Arquivo 'Pessoas'"):
            with st.spinner("Processando... Por favor, aguarde."):
                try:
                    # Validate NOME mapping before proceeding
                    if not user_col_mapping["NOME"]:
                        st.warning("A coluna 'NOME' é obrigatória para a distribuição de leads.")
                        return

                    # Apply mapping and rename DataFrame
                    df_leads_mapped = df_raw_leads.copy()
                    for expected, actual in user_col_mapping.items():
                        if actual: # Only process if a column was selected
                            if actual in df_leads_mapped.columns:
                                df_leads_mapped.rename(columns={actual: expected}, inplace=True)
                            else:
                                st.warning(f"A coluna '{actual}' selecionada para '{expected}' não foi encontrada no arquivo. Verifique o mapeamento.")
                                return

                    # Validate if NOME column exists after mapping
                    if "NOME" not in df_leads_mapped.columns:
                        st.warning("A coluna 'NOME' é obrigatória para a distribuição de leads e não foi mapeada corretamente.")
                        return

                    # Limpa e filtra pelo número de WhatsApp
                    if "Whats" in df_leads_mapped.columns:
                        initial_rows = len(df_leads_mapped)
                        df_leads_mapped["Whats"] = df_leads_mapped["Whats"].apply(clean_phone_number)
                        df_leads_mapped.dropna(subset=["Whats"], inplace=True)
                        final_rows = len(df_leads_mapped)
                        st.info(f"{initial_rows - final_rows} linhas foram removidas por não conterem um número de WhatsApp válido.")
                    else:
                        st.warning("A coluna 'Whats' não foi mapeada. Nenhuma filtragem por WhatsApp foi aplicada.")

                    if df_leads_mapped.empty:
                        st.warning("Após a filtragem, não restaram leads para distribuir.")
                        return

                    if not effective_consultores:
                        st.warning("Nenhum consultor selecionado após a aplicação dos filtros. Ajuste suas seleções.")
                        return

                    # Clean CEL column (apply same phone cleaning as Whats)
                    if "CEL" in df_leads_mapped.columns:
                        # Para o campo 'Celular' preservamos todos os dígitos completos
                        # (evita remover o primeiro dígito do DDD). Use preserve_full=True.
                        df_leads_mapped["CEL"] = df_leads_mapped["CEL"].apply(lambda x: clean_phone_number(x, preserve_full=True))
                        # Replace NaN with empty string for easier usage later
                        df_leads_mapped["CEL"] = df_leads_mapped["CEL"].fillna("")
                    
                    # --- Agendor Specific Logic ---
                    # Deduplicate by WhatsApp
                    if "Whats" in df_leads_mapped.columns:
                        df_leads_mapped.drop_duplicates(subset=["Whats"], keep='first', inplace=True)
                        st.info(f"Leads após desduplicação por WhatsApp: {len(df_leads_mapped)}")

                    # Prepare for Agendor output
                    colunas_output = [
                        "Nome", "CPF", "Empresa", "Cargo", "Aniversário", "Ano de nascimento", 
                        "Usuário responsável", "Categoria", "Origem", "Descrição", "E-mail", 
                        "WhatsApp", "Telefone", "Celular", "Fax", "Ramal", "CEP", "País", 
                        "Estado", "Cidade", "Bairro", "Rua", "Número", "Complemento", 
                        "Produto", "Facebook", "Twitter", "LinkedIn", "Skype", "Instagram", "Ranking"
                    ]
                    
                    # --- Lógica de Geração e Download ---
                    # Armazena os arquivos gerados em memória
                    generated_files = {}

                    leads_processados = 0
                    total_leads = len(df_leads_mapped)

                    # Debug lines removed from UI for cleaner UX

                    # If exactly one consultant is selected and the user did not request forced splitting,
                    # create a single file containing all leads for that consultant.
                    # Use the local variable 'force_split' which is safely initialized above.
                    if len(effective_consultores) == 1 and not force_split:
                        consultor = effective_consultores[0]
                        dados_finais = []
                        consultor_formatado = consultor.lower().replace(' ', '.')
                        df_lote = df_leads_mapped.copy()
                        for _, row in df_lote.iterrows():
                            whatsapp_val = row.get("Whats")
                            whatsapp_str = f"+55{str(whatsapp_val).strip()}" if whatsapp_val and pd.notna(whatsapp_val) and str(whatsapp_val).strip() else ""
                            celular_val = row.get("CEL")
                            celular_str = str(celular_val) if celular_val and pd.notna(celular_val) else ""
                            
                            
                            # Lógica para Descrição
                            descricao_val = ""
                            if desc_mode == "Valor Fixo":
                                descricao_val = default_descricao.strip()
                            elif col_descricao and col_descricao in row:
                                val_col = row.get(col_descricao)
                                descricao_val = str(val_col).strip() if pd.notna(val_col) else ""
                            
                            # Fallback original se estiver vazio
                            if not descricao_val:
                                descricao_val = row.get("Razao Social") or row.get("Fantasia") or row.get("Empresa") or ""

                            # Lógica para UF
                            uf_val = ""
                            if uf_mode == "Valor Fixo":
                                uf_val = default_uf
                            elif col_uf and col_uf in row:
                                val_uf = row.get(col_uf)
                                uf_val = str(val_uf).strip()[0:2].upper() if pd.notna(val_uf) else ""
                            
                            if not uf_val: # Fallback safe
                                uf_val = "MS"
                            cep_val = ""
                            if "CEP" in row and pd.notna(row.get("CEP")):
                                cep_val = normalize_cep(row.get("CEP"))
                            linha = {col: "" for col in colunas_output}
                            linha.update({
                                "Nome": row.get("NOME", ""),
                                "Cargo": default_cargo,
                                "Usuário responsável": consultor_formatado,
                                "Categoria": "Lead",
                                "Origem": "Reobote",
                                "Descrição": descricao_val,
                                "WhatsApp": whatsapp_str,
                                "Celular": celular_str,
                                "Estado": uf_val,
                                "Cidade": row.get("Cidade", ""),
                                "Bairro": row.get("Bairro", ""),
                                "Rua": row.get("Rua", ""),
                                "Número": row.get("Número", ""),
                                "Complemento": row.get("Complemento", ""),
                                "CEP": cep_val
                            })
                            dados_finais.append(linha)

                        df_final_consultor = pd.DataFrame(dados_finais, columns=colunas_output)
                        output_excel_consultor = generate_excel_buffer(df_final_consultor, sheet_name='Pessoas')

                        # Determine localidade for filename (safer logic)
                        localidade = determine_localidade(user_col_mapping, df_lote, default="CG")

                        nicho_formatado = nicho_valor.upper().replace(' ', '_')
                        primeiro_nome = consultor.split(' ')[0].upper()
                        data_formatada = datetime.now().strftime('%d-%m-%Y')
                        # Nome do arquivo: usar apenas o nicho e o primeiro nome do consultor
                        nome_arquivo_agendor = f"PESSOAS_{nicho_formatado}_{primeiro_nome}_{data_formatada}.xlsx"
                        generated_files[nome_arquivo_agendor] = output_excel_consultor.getvalue()
                        leads_processados = total_leads
                    else:
                        # Logic for multiple consultants or forced split
                        # Accumulate data first to avoid file overwrites
                        consultant_buffer = {c: [] for c in effective_consultores}
                        
                        while leads_processados < total_leads:
                            for consultor in effective_consultores:
                                if leads_processados >= total_leads:
                                    break

                                inicio_lote = leads_processados
                                fim_lote = leads_processados + leads_por_consultor
                                df_lote = df_leads_mapped.iloc[inicio_lote:fim_lote].copy()

                                if not df_lote.empty:
                                    consultor_formatado = consultor.lower().replace(' ', '.')
                                    for _, row in df_lote.iterrows():
                                        whatsapp_val = row.get("Whats")
                                        whatsapp_str = f"+55{str(whatsapp_val).strip()}" if whatsapp_val and pd.notna(whatsapp_val) and str(whatsapp_val).strip() else ""
                                        celular_val = row.get("CEL")
                                        celular_str = str(celular_val) if celular_val and pd.notna(celular_val) else ""
                                        
                                        # Lógica para Descrição
                                        descricao_val = ""
                                        if desc_mode == "Valor Fixo":
                                            descricao_val = default_descricao.strip()
                                        elif col_descricao and col_descricao in row:
                                            val_col = row.get(col_descricao)
                                            descricao_val = str(val_col).strip() if pd.notna(val_col) else ""
                                        
                                        # Fallback original
                                        if not descricao_val:
                                            descricao_val = row.get("Razao Social") or row.get("Fantasia") or row.get("Empresa") or ""
                                            
                                        # Lógica para UF
                                        uf_val = ""
                                        if uf_mode == "Valor Fixo":
                                            uf_val = default_uf
                                        elif col_uf and col_uf in row:
                                            val_uf = row.get(col_uf)
                                            uf_val = str(val_uf).strip()[0:2].upper() if pd.notna(val_uf) else ""
                                        
                                        if not uf_val:
                                            uf_val = "MS"
                                        cep_val = ""
                                        if "CEP" in row and pd.notna(row.get("CEP")):
                                            cep_val = normalize_cep(row.get("CEP"))
                                        
                                        linha = {col: "" for col in colunas_output}
                                        linha.update({
                                            "Nome": row.get("NOME", ""),
                                            "Cargo": default_cargo,
                                            "Usuário responsável": consultor_formatado,
                                            "Categoria": "Lead",
                                            "Origem": "Reobote",
                                            "Descrição": descricao_val,
                                            "WhatsApp": whatsapp_str,
                                            "Celular": celular_str,
                                            "Estado": uf_val,
                                            "Cidade": row.get("Cidade", ""),
                                            "Bairro": row.get("Bairro", ""),
                                            "Rua": row.get("Rua", ""),
                                            "Número": row.get("Número", ""),
                                            "Complemento": row.get("Complemento", ""),
                                            "CEP": cep_val
                                        })
                                        consultant_buffer[consultor].append(linha)

                                    leads_processados += len(df_lote)

                        # Generate files from buffer
                        for consultor, dados_finais in consultant_buffer.items():
                            if dados_finais:
                                df_final_consultor = pd.DataFrame(dados_finais, columns=colunas_output)
                                output_excel_consultor = generate_excel_buffer(df_final_consultor, sheet_name='Pessoas')

                                nicho_formatado = nicho_valor.upper().replace(' ', '_')
                                primeiro_nome = consultor.split(' ')[0].upper()
                                data_formatada = datetime.now().strftime('%d-%m-%Y')
                                nome_arquivo_agendor = f"PESSOAS_{nicho_formatado}_{primeiro_nome}_{data_formatada}.xlsx"
                                generated_files[nome_arquivo_agendor] = output_excel_consultor.getvalue()

                    # --- Lógica de Download e Handoff ---
                    if not generated_files:
                        st.warning("Nenhum arquivo foi gerado. Verifique os filtros e os dados de entrada.")
                        return

                    # Salva os arquivos gerados no estado da sessão para o handoff
                    st.session_state.generated_pessoas_files = generated_files
                    # Persiste o DataFrame original para permitir a reconciliação de erros posterir
                    st.session_state.last_agendor_df = df_leads_mapped.copy()
                    st.session_state.last_agendor_col_mapping = user_col_mapping.copy()

                    st.success(f"Processo concluído! {len(generated_files)} arquivo(s) de pessoas para Agendor foram gerados.")

                    # Opções de Download
                    col1, col2 = st.columns(2)
                    with col1:
                        # Se for apenas um arquivo, oferece o download direto
                        if len(generated_files) == 1:
                            file_name, file_data = list(generated_files.items())[0]
                            st.download_button(
                                label=f"Baixar Arquivo para Agendor (.xlsx)",
                                data=file_data,
                                file_name=file_name,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_single_agendor"
                            )
                        # Se forem vários arquivos, agrupa em um ZIP
                        else:
                            zip_buffer = io.BytesIO()
                            with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                                for file_name, file_data in generated_files.items():
                                    # Extrai o nome do consultor do nome do arquivo para encontrar a equipe
                                    parts = file_name.split('_')
                                    consultor_nome_no_arquivo = ""
                                    if len(parts) > 3:
                                        consultor_nome_no_arquivo = parts[-2].upper()

                                    nome_equipe = "Outros" # Padrão
                                    # Buscar equipe do consultor via JSON
                                    for equipe in carregar_equipes():
                                        for consultor in equipe["consultores"]:
                                            if consultor.split(' ')[0].upper() == consultor_nome_no_arquivo:
                                                nome_equipe = equipe["nome"]
                                                break
                                    zip_file.writestr(f"{nome_equipe}/{file_name}", file_data)
                            
                            zip_filename = f"Pessoas_Agendor_Distribuicao_{datetime.now().strftime('%d-%m-%Y')}.zip"
                            st.download_button(
                                label="Baixar Todos os Arquivos (ZIP)",
                                data=zip_buffer.getvalue(),
                                file_name=zip_filename,
                                mime="application/zip",
                                key="download_zip_agendor"
                            )
                    
                    # Botão para Handoff
                    with col2:
                        if st.session_state.get('generated_pessoas_files') and st.button("Continuar e Gerar Negócios ➡️"):
                            st.session_state.handoff_active = True
                            st.session_state.source_for_negocios = 'handoff'
                            # Mensagens para guiar o usuário em vez de rerun
                            st.success("✅ Leads prontos para a próxima etapa!")
                            st.info("ℹ️ Agora, clique na aba 'Gerador de Negócios para Robôs' para gerar os arquivos de negócios.")

                except Exception as e:
                    import traceback
                    st.error(f"Ocorreu um erro durante o processamento: {e}")
                    st.code(traceback.format_exc())


    # --- Seção de Reconciliação (Ciclo Fechado) ---
    # Moved outside the 'Gerar' button scope to persist on interactions
    st.markdown("---")
    
    try:
        # Mantém o expander aberto se tivermos gerado arquivos agora OU se já estivermos no meio da reconciliação
        recon_active = bool(st.session_state.get('generated_pessoas_files') or st.session_state.get('recon_complete'))
    
        with st.expander("🛠️ Validação de Erros Agendor (Ciclo Fechado)", expanded=recon_active):
            st.info("Suba o 'Relatório de Erros' gerado pelo Agendor para separar Duplicidades (Lixo) de Erros Recuperáveis.")
        
            # Recuperação de Sessão ou Upload Manual do Original
            df_original_source = st.session_state.get('last_agendor_df')
            
            # Layout Inteligente: Se já temos o original, mostra apenas upload de erro (full width).
            # Se não temos, divide em 2 colunas para subir o original também.
            
            erro_file = None
            
            if df_original_source is not None:
                # Caso Simples: Só precisa do arquivo de erro
                st.success("✅ Arquivo Original carregado da sessão atual.")
                erro_file = st.file_uploader("Upload Relatório de Erros Agendor (.xlsx)", type=["xlsx"])
            else:
                # Caso Completo: Precisa dos dois
                c_err, c_orig = st.columns(2)
                with c_err:
                     erro_file = st.file_uploader("Upload Relatório de Erros Agendor (.xlsx)", type=["xlsx"])
                with c_orig:
                     orig_file = st.file_uploader("Upload Arquivo Original (O que você enviou)", type=["xlsx", "csv"])
                     if orig_file:
                        try:
                            if orig_file.name.endswith('.csv'):
                                try:
                                    string_data = orig_file.getvalue().decode('utf-8')
                                    sniffer = csv.Sniffer()
                                    dialect = sniffer.sniff(string_data[:1024])
                                    delimiter = dialect.delimiter
                                except:
                                    delimiter = ',' 
                                orig_file.seek(0)
                                df_original_source = pd.read_csv(orig_file, delimiter=delimiter, dtype=str)
                            else:
                                df_original_source = pd.read_excel(orig_file, dtype=str)
                            
                            if "Whats" in df_original_source.columns:
                                df_original_source["Whats"] = df_original_source["Whats"].apply(lambda x: format_phone_for_whatsapp_business(x, include_country_code=False)[0])
                                
                            st.success("Arquivo Original Carregado.")
                        except Exception as e:
                            st.error(f"Erro ao ler original: {e}")

        # Botão de Análise
        if erro_file and df_original_source is not None:
            if st.button("Analisar e Separar Erros"):
                try:
                    df_err = pd.read_excel(erro_file, dtype=str)
                    df_safe, df_manual, stats = process_agendor_report(df_original_source, df_err)
                    
                    # Salva no estado
                    st.session_state.recon_df_safe = df_safe
                    st.session_state.recon_df_manual = df_manual
                    st.session_state.recon_stats = stats
                    st.session_state.recon_complete = True
                    st.rerun() # Refresh para mostrar editores
                except Exception as e:
                    st.error(f"Erro ao processar reconciliação: {e}")
                    
        # Exibição dos Resultados e Editor
        if st.session_state.get('recon_complete'):
            stats = st.session_state.recon_stats
            
            # Métricas Visuais
            c1, c2, c3 = st.columns(3)
            c1.metric("Duplicidades Removidas", stats['duplicates_removed'], delta_color="normal")
            c2.metric("Erros para Ajuste Manual", stats['manual_fix_needed'], delta_color="off")
            c3.metric("Leads Salvos (Sem Erro)", stats['safe_total'], delta="+OK")
            
            st.write("---")
            
            # Área de Edição (War Room)
            df_manual = st.session_state.recon_df_manual
            
            if not df_manual.empty:
                st.warning(f"⚠️ **{len(df_manual)} leads precisam de ajuste.** Edite os campos abaixo (ex: Cidade, Email) e confirme.")
                
                # Configuração do Editor para evitar bugs de float
                column_config = {
                    column: st.column_config.TextColumn(column) 
                    for column in df_manual.columns
                }
                # Destaque para Motivo
                column_config["MOTIVO_ERRO"] = st.column_config.TextColumn("Motivo do Erro", disabled=True)
                
                edited_df = st.data_editor(
                    df_manual,
                    column_config=column_config,
                    use_container_width=True,
                    num_rows="dynamic",
                    key="editor_reconciliacao"
                )
                
                if st.button("✅ Confirmar Correções e Gerar Arquivo Final"):
                    # Fusão: Safe + Edited
                    df_safe = st.session_state.recon_df_safe
                    df_final_reconciled = pd.concat([df_safe, edited_df], ignore_index=True)
                    
                    # Gerar Excel
                    output_buffer = generate_excel_buffer(df_final_reconciled, sheet_name='Pessoas')
                    
                    # Salvar no Session State para persistencia do botão
                    timestamp = datetime.now().strftime('%H%M')
                    st.session_state.recon_final_bytes = output_buffer.getvalue()
                    st.session_state.recon_final_name = f"PESSOAS_CORRIGIDO_{timestamp}.xlsx"
                    st.session_state.recon_download_ready = True
                    st.rerun()

                # Botão de Download Persistente (fora do if st.button)
                if st.session_state.get("recon_download_ready"):
                    st.success(f"Arquivo Regenerado com Sucesso!")
                    
                    st.download_button(
                        label="⬇️ Baixar Arquivo Corrigido (Substituir Original)",
                        data=st.session_state.recon_final_bytes,
                        file_name=st.session_state.recon_final_name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="dl_recon_persistente"
                    )
                    
            else:
                st.success("🎉 Nenhum erro manual pendente! Todos os problemas eram duplicidades e foram removidos.")
                if st.button("Gerar Arquivo Limpo (Sem Duplicidades)"):
                     # Apenas Safe
                    df_safe = st.session_state.recon_df_safe
                    output_buffer = generate_excel_buffer(df_safe, sheet_name='Pessoas')
                    
                    st.download_button(
                        label="⬇️ Baixar Arquivo Limpo",
                        data=output_buffer.getvalue(),
                        file_name=f"PESSOAS_LIMPO_SEM_DUPLICIDADES.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

    except Exception as e:
        import traceback
        st.error(f"Ocorreu um erro durante a reconciliação: {e}")
        st.code(traceback.format_exc())


# Note: `carregar_consultores` / `salvar_consultores` and `CONSULTORES_FILE`
# are defined earlier in the module. Duplicate definitions were removed to
# avoid confusion and accidental redefinition.

def aba_gerenciar_consultores():
    st.header("Gerenciar Consultores")
    consultores = carregar_consultores()
    equipes = carregar_equipes()




    # Formulário para adicionar novo consultor (só mostra se não está editando)
    if "edit_idx" not in st.session_state:
        st.subheader("Adicionar novo consultor")
        with st.form("add_consultor_form"):
            novo_usuario = st.text_input("Nome de usuário do consultor")
            novo_consultor = st.text_input("Nome do consultor (exibição)")
            submitted_add = st.form_submit_button("Adicionar consultor")
            if submitted_add:
                try:
                    if not novo_usuario.strip() or not novo_consultor.strip():
                        st.warning("Preencha todos os campos para adicionar um consultor.")
                    elif any(c["usuario"] == novo_usuario and c["consultor"] == novo_consultor for c in consultores):
                        st.warning("Já existe um consultor com esse usuário e nome.")
                    else:
                        consultores.append({"usuario": novo_usuario, "consultor": novo_consultor})
                        salvar_consultores(consultores)
                        st.success(f"Consultor '{novo_consultor}' adicionado!")
                        try:
                            st.rerun()
                        except AttributeError:
                            st.warning("Não foi possível recarregar a página automaticamente. Atualize manualmente.")
                except Exception as e:
                    st.error(f"Erro ao adicionar consultor: {e}")

    # Expander deve ficar aberto se estiver editando
    expanded_consultores = "edit_idx" in st.session_state

    with st.expander("Lista de consultores cadastrados", expanded=expanded_consultores):
        if consultores:
            for idx, c in enumerate(consultores):
                unique_id = f"{c['usuario']}__{c['consultor']}__{idx}"
                if "edit_idx" in st.session_state and st.session_state["edit_idx"] == idx:
                    # Modo edição inline
                    st.markdown(f"**Editando consultor {c['consultor']} (usuário: {c['usuario']})**")
                    with st.form(f"edit_consultor_form_{idx}"):
                        novo_usuario = st.text_input("Nome de usuário", value=st.session_state["edit_usuario"])
                        novo_consultor = st.text_input("Nome do consultor (exibição)", value=st.session_state["edit_consultor"])
                        colsave, colcancel = st.columns([2,2])
                        with colsave:
                            submitted_edit = st.form_submit_button("Salvar alterações")
                        with colcancel:
                            cancel_edit = st.form_submit_button("Cancelar")
                        if submitted_edit:
                            try:
                                if not novo_usuario.strip() or not novo_consultor.strip():
                                    st.warning("Preencha todos os campos para editar o consultor.")
                                elif any(i != st.session_state["edit_idx"] and c["usuario"] == novo_usuario and c["consultor"] == novo_consultor for i, c in enumerate(consultores)):
                                    st.warning("Já existe um consultor com esse usuário e nome.")
                                else:
                                    idx = st.session_state["edit_idx"]
                                    antigo_nome = consultores[idx]["consultor"]
                                    consultores[idx] = {"usuario": novo_usuario, "consultor": novo_consultor}
                                    salvar_consultores(consultores)
                                    for equipe in equipes:
                                        if antigo_nome in equipe["consultores"]:
                                            equipe["consultores"].remove(antigo_nome)
                                            equipe["consultores"].append(novo_consultor)
                                    salvar_equipes(equipes)
                                    st.success("Consultor atualizado!")
                                    del st.session_state["edit_idx"]
                                    del st.session_state["edit_usuario"]
                                    del st.session_state["edit_consultor"]
                                    try:
                                        st.rerun()
                                    except AttributeError:
                                        st.warning("Não foi possível recarregar a página automaticamente. Atualize manualmente.")
                            except Exception as e:
                                st.error(f"Erro ao editar consultor: {e}")
                        if cancel_edit:
                            del st.session_state["edit_idx"]
                            del st.session_state["edit_usuario"]
                            del st.session_state["edit_consultor"]
                            try:
                                st.rerun()
                            except AttributeError:
                                st.warning("Não foi possível recarregar a página automaticamente. Atualize manualmente.")
                else:
                    col1, col2, col3 = st.columns([4, 3, 2])
                    with col1:
                        st.write(f"{idx+1}. {c['consultor']} (usuário: {c['usuario']})")
                    with col2:
                        if st.button("Editar", key=f"edit_{unique_id}"):
                            st.session_state["edit_idx"] = idx
                            st.session_state["edit_usuario"] = c["usuario"]
                            st.session_state["edit_consultor"] = c["consultor"]
                            st.session_state["abrir_expander_consultores"] = True
                            try:
                                st.rerun()
                            except AttributeError:
                                st.warning("Não foi possível recarregar a página automaticamente. Atualize manualmente.")
                    with col3:
                        if st.button("Excluir", key=f"delete_{unique_id}"):
                            try:
                                consultores.pop(idx)
                                salvar_consultores(consultores)
                                for equipe in equipes:
                                    if c["consultor"] in equipe["consultores"]:
                                        equipe["consultores"].remove(c["consultor"])
                                salvar_equipes(equipes)
                                st.success("Consultor excluído!")
                                try:
                                    st.rerun()
                                except AttributeError:
                                    st.warning("Não foi possível recarregar a página automaticamente. Atualize manualmente.")
                            except Exception as e:
                                st.error(f"Erro ao excluir consultor: {e}")
        else:
            st.info("Nenhum consultor cadastrado ainda.")


    st.markdown("---")
    with st.expander("Gerenciar Equipes", expanded=False):
        equipes = carregar_equipes()
        with st.form("add_equipe_form"):
            nome_equipe = st.text_input("Nome da equipe")
            submitted_equipe = st.form_submit_button("Adicionar equipe")
            if submitted_equipe and nome_equipe:
                try:
                    if not any(eq["nome"] == nome_equipe for eq in equipes):
                        equipes.append({"nome": nome_equipe, "consultores": []})
                        salvar_equipes(equipes)
                        st.success(f"Equipe '{nome_equipe}' adicionada!")
                        try:
                            st.rerun()
                        except AttributeError:
                            st.warning("Não foi possível recarregar a página automaticamente. Atualize manualmente.")
                    else:
                        st.warning("Já existe uma equipe com esse nome.")
                except Exception as e:
                    st.error(f"Erro ao adicionar equipe: {e}")

        for idx, equipe in enumerate(equipes):
            st.subheader(f"Equipe: {equipe['nome']}")
            consultores_nomes = [c["consultor"] for c in consultores]
            consultores_na_equipe = equipe["consultores"]
            consultores_disponiveis = [c for c in consultores_nomes if c not in consultores_na_equipe]
            add_col, del_col, edit_col = st.columns([4,2,2])
            with add_col:
                novo_consultor = st.selectbox(f"Adicionar consultor à {equipe['nome']}", ["-- Selecione --"] + consultores_disponiveis, key=f"add_consultor_{idx}")
                if novo_consultor != "-- Selecione --" and st.button(f"Adicionar à {equipe['nome']}", key=f"btn_add_consultor_{idx}"):
                    try:
                        equipe["consultores"].append(novo_consultor)
                        salvar_equipes(equipes)
                        st.success(f"Consultor '{novo_consultor}' adicionado à equipe '{equipe['nome']}'!")
                        try:
                            st.rerun()
                        except AttributeError:
                            st.warning("Não foi possível recarregar a página automaticamente. Atualize manualmente.")
                    except Exception as e:
                        st.error(f"Erro ao adicionar consultor à equipe: {e}")
            with del_col:
                if st.button(f"Excluir equipe", key=f"delete_equipe_{idx}"):
                    try:
                        equipes.pop(idx)
                        salvar_equipes(equipes)
                        st.success("Equipe excluída!")
                        try:
                            st.rerun()
                        except AttributeError:
                            st.warning("Não foi possível recarregar a página automaticamente. Atualize manualmente.")
                    except Exception as e:
                        st.error(f"Erro ao excluir equipe: {e}")
            with edit_col:
                if st.button(f"Renomear equipe", key=f"rename_equipe_{idx}"):
                    st.session_state["edit_equipe_idx"] = idx
                    st.session_state["edit_equipe_nome"] = equipe["nome"]
            st.write("**Consultores na equipe:**")
            for cidx, nome_c in enumerate(equipe["consultores"]):
                ccol1, ccol2 = st.columns([6,2])
                with ccol1:
                    st.write(f"- {nome_c}")
                with ccol2:
                    if st.button(f"Remover", key=f"remover_{idx}_{cidx}"):
                        try:
                            equipe["consultores"].remove(nome_c)
                            salvar_equipes(equipes)
                            st.success(f"Consultor '{nome_c}' removido da equipe '{equipe['nome']}'!")
                            try:
                                st.rerun()
                            except AttributeError:
                                st.warning("Não foi possível recarregar a página automaticamente. Atualize manualmente.")
                        except Exception as e:
                            st.error(f"Erro ao remover consultor da equipe: {e}")
            if "edit_equipe_idx" in st.session_state and st.session_state["edit_equipe_idx"] == idx:
                with st.form(f"form_rename_equipe_{idx}"):
                    novo_nome = st.text_input("Novo nome da equipe", value=st.session_state["edit_equipe_nome"])
                    submitted_rename = st.form_submit_button("Salvar nome")
                    if submitted_rename and novo_nome:
                        try:
                            equipes[idx]["nome"] = novo_nome
                            salvar_equipes(equipes)
                            st.success("Nome da equipe atualizado!")
                            del st.session_state["edit_equipe_idx"]
                            del st.session_state["edit_equipe_nome"]
                            st.experimental_rerun()
                        except Exception as e:
                            st.error(f"Erro ao renomear equipe: {e}")



def main():
    st.set_page_config(page_title="Automação de Listas", layout="wide")

    # Header (minimal)
    st.markdown(
        """
        <div style='display:flex;align-items:center;gap:12px'>
          <div style='width:44px;height:44px;border-radius:8px;background:#4B8BBE;display:flex;align-items:center;justify-content:center;color:#fff;font-weight:700'>A</div>
          <div>
            <div style='font-size:20px;font-weight:600;color:#e6eef8'>Automação de Listas</div>
            <div style='font-size:12px;color:#94a3b8'>Minimal · elegante · eficiente</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Global dark theme CSS (visual only)
    st.markdown(
        """
        <style>
          /* Hide default bits */
          #MainMenu {visibility: hidden;}
          footer {visibility: hidden;}

          /* App background and text */
          [data-testid="stAppViewContainer"] {
            background: linear-gradient(180deg,#071226 0%, #07122a 100%);
            color: #e6eef8;
          }

          /* Sidebar styling */
          section[data-testid="stSidebar"] {
            background: linear-gradient(180deg,#061224 0%, #071226 100%);
            border-right: 1px solid rgba(255,255,255,0.03);
            padding-top: 10px;
          }
          section[data-testid="stSidebar"] .css-1d391kg, section[data-testid="stSidebar"] .css-1lcbmhc {
            color: #cbd5e1;
          }

          /* Option menu overrides (streamlit_option_menu classes) */
          .option-menu { background: transparent !important; }
          .option-menu .nav-link { color: #cbd5e1 !important; }
          .option-menu .nav-link:hover { background: rgba(255,255,255,0.02) !important; }
          .option-menu .nav-link-selected { background: #12324a !important; color: #e6eef8 !important; font-weight:600 !important; box-shadow: 0 6px 18px rgba(12,44,66,0.35) !important; }

          /* Info / Alert boxes look like glass cards */
          .stAlert, .css-1tq5r2k { background: rgba(255,255,255,0.02) !important; border: 1px solid rgba(255,255,255,0.03) !important; color: #dbeafe !important; border-radius: 10px !important; padding: 10px 14px !important; }

          /* File uploader and input controls */
          .stFileUploader, .css-1hynsf2, .css-1y4p8pa { background: rgba(255,255,255,0.02) !important; border: 1px solid rgba(255,255,255,0.03) !important; border-radius: 10px !important; padding: 12px !important; }

          /* Buttons */
          .stButton>button {
            background: linear-gradient(90deg,#4B8BBE,#3B82F6) !important;
            color:#fff !important;
            border-radius:8px !important;
            padding:8px 14px !important;
            border: none !important;
            box-shadow: 0 6px 18px rgba(59,130,246,0.12) !important;
          }
          .stButton>button:hover { transform: translateY(-1px); }

          /* Dataframe / tables */
          [data-testid="stDataFrame"] { background: rgba(255,255,255,0.02); border-radius:8px; padding:8px; }

          /* Muted text */
          .muted, small { color:#94a3b8; }

          /* Headings */
          .css-2trqyj h1, .css-2trqyj h2, h1, h2, h3 { color:#e6eef8; font-weight:600; }

          /* Make controls slightly higher contrast */
          .stSelectbox > div[role="combobox"] { background: rgba(255,255,255,0.01) !important; border-radius:8px !important; padding:6px 8px !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Sidebar navigation
    if "sidebar_open" not in st.session_state:
        st.session_state.sidebar_open = True

    with st.sidebar:
        st.markdown("<div style='padding:8px 6px'><strong style='font-size:14px'>Navegação</strong></div>", unsafe_allow_html=True)
        selected = option_menu(
            "Navegação",
            ["Higienização de dados", "Divisor de Listas Diárias - Auto", "Gerador de Negócios para Robôs", "Automação Pessoas Agendor", "Gerenciar Consultores/Equipes"],
            icons=["list-task", "columns-gap", "robot", "people", "person-lines-fill"],
            menu_icon="cast",
            default_index=0,
            styles={
                "container": {"padding": "4px 2px", "background-color": "#ffffff00"},
                "icon": {"color": "#6b7280", "font-size": "18px"},
                "nav-link": {"font-size": "14px", "text-align": "left", "margin": "4px 0px", "color": "#cbd5e1", "background-color": "transparent", "border-radius": "6px", "height": "44px", "display": "flex", "align-items": "center", "padding-left": "8px"},
                "nav-link-selected": {"background-color": "#12324a", "color": "#e6eef8", "font-weight": "600"},
            }
        )
        page = selected

    # Page routing (no logic changes)
    if page == "Higienização de dados":
        aba_higienizacao()
    elif page == "Divisor de Listas Diárias - Auto":
        aba_divisor_listas()
    elif page == "Gerador de Negócios para Robôs":
        aba_gerador_negocios_robos()
    elif page == "Automação Pessoas Agendor":
        aba_automacao_pessoas_agendor()
    elif page == "Gerenciar Consultores/Equipes":
        aba_gerenciar_consultores()

    # (Removed duplicate sidebar block and duplicate page routing to avoid rendering pages twice.)

if __name__ == "__main__":
    main()
