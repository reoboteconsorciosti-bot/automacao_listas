from fpdf import FPDF
import pandas as pd
import io
import os
import streamlit as st

class PDF(FPDF):
    def header(self):
        # Exibe o título apenas na primeira página
        if self.page_no() == 1:
            try:
                self.set_font('NotoSans', 'B', 10)
            except RuntimeError:
                self.set_font('Arial', 'B', 10)
            self.cell(0, 5, self.title, 0, 1, 'C')
        else:
            # Adiciona um espaço em branco para manter o alinhamento da tabela nas páginas seguintes
            self.ln(5)

    def footer(self):
        # Posição do rodapé reduzida para diminuir a margem inferior
        self.set_y(-10)
        try:
            self.set_font('NotoSans', 'I', 8)
        except RuntimeError:
            self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Página {self.page_no()}', 0, 0, 'C')

def create_pdf_robust(df, title="Relatório", cols_to_center=None, cols_single_checkbox=None, cols_double_checkbox=None):
    if cols_to_center is None:
        cols_to_center = []
    if cols_single_checkbox is None:
        cols_single_checkbox = []
    if cols_double_checkbox is None:
        cols_double_checkbox = []
    if df.empty:
        st.warning(f"Tentativa de gerar PDF para '{title}' com dados vazios. PDF não gerado.")
        return None

    pdf = PDF(orientation='L', unit='mm', format='A4')
    pdf.title = title
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    font_path = os.path.join(base_dir, 'fonts', 'NotoSans-Regular.ttf')
    font_bold_path = os.path.join(base_dir, 'fonts', 'NotoSans-Bold.ttf')

    try:
        if not os.path.exists(font_path) or not os.path.exists(font_bold_path):
            st.warning("Arquivos de fonte Noto Sans não encontrados. Usando Arial como fallback.")
            pdf.set_font('Arial', '', 8)
        else:
            pdf.add_font('NotoSans', '', font_path, uni=True)
            pdf.add_font('NotoSans', 'B', font_bold_path, uni=True)
    except Exception as e:
        st.error(f"Ocorreu um erro crítico ao carregar as fontes: {e}")
        return None

    pdf.add_page()
    
    margin = 5  # Margem da página reduzida para 5mm
    page_width = pdf.w - 2 * margin
    
    headers = df.columns.tolist()
    num_columns = len(headers)
    if num_columns == 0:
        return None

    # --- LÓGICA DE ESCALONAMENTO E LARGURA DINÂMICA ---
    # Passo 1: Determinar a largura "ideal" de cada coluna baseada no conteúdo
    col_ideal_widths = {}
    
    # Define fontes provisoriamente para medir os textos
    try:
        pdf.set_font('NotoSans', 'B', 10) # Usa a fonte do cabeçalho
    except:
        pdf.set_font('Arial', 'B', 10)
        
    for header in headers:
        # Largura do cabeçalho
        max_width = pdf.get_string_width(str(header)) + 4 # 4mm de padding interno
        
        # Opcional: para colunas de checkbox garantir um tamanho fixo
        if header in cols_single_checkbox:
            max_width = max(max_width, pdf.get_string_width("[  ]") + 4)
        elif header in cols_double_checkbox:
            max_width = max(max_width, pdf.get_string_width("[  ]   [  ]") + 4)
            
        col_ideal_widths[header] = max_width

    # Medir também algumas linhas do corpo para ter uma média (Head 50 ou amostra)
    try:
        pdf.set_font('NotoSans', '', 9)
    except:
        pdf.set_font('Arial', '', 9)
        
    for _, row in df.head(50).iterrows(): # Amostra para não demorar muito em dfs gigantes
        for header in headers:
            if header not in cols_single_checkbox and header not in cols_double_checkbox:
                cell_text = str(row.get(header, ''))
                w = pdf.get_string_width(cell_text) + 4
                if w > col_ideal_widths[header]:
                    col_ideal_widths[header] = w
                    
    # Cap limites mínimos e máximos ideais para evitar aberrações
    for header in headers:
        w = col_ideal_widths[header]
        col_ideal_widths[header] = max(15, min(w, 80)) # min 15mm, máx 80mm

    total_ideal_width = sum(col_ideal_widths.values())

    # Passo 2: Calcular Fator de Escala
    # A folha A4 Landscape tem ~297mm. As margens são subtraídas (page_width)
    col_widths = {}
    if total_ideal_width > page_width:
        # Se ultrapassar, encolhe todas proporcionalmente
        scale_factor = page_width / total_ideal_width
        for h in headers:
            col_widths[h] = col_ideal_widths[h] * scale_factor
    else:
        # Se for menor, podemos expandir proporcionalmente para preencher a tela ou deixar como está.
        # Vamos deixar alinhado preenchendo a tela para ficar bonito
        scale_factor = page_width / total_ideal_width
        for h in headers:
            col_widths[h] = col_ideal_widths[h] * scale_factor

    # --- FIM DA LÓGICA DE LARGURA ---

    pdf.set_line_width(0.1) # Bordas mais finas
    pdf.set_x(margin)
    # Cabeçalho Azul Escuro com texto Branco (Estilo Premium)
    pdf.set_fill_color(22, 54, 92) # Azul escuro profissional
    pdf.set_text_color(255, 255, 255) # Texto branco no cabeçalho
    
    # --- FORMATAÇÃO DO CABEÇALHO ---
    try:
        
        # Ajusta dinamicamente a fonte se as colunas ficarem muito exprimidas
        if scale_factor < 0.7:
             pdf.set_font('NotoSans', 'B', 8)
        else:
             pdf.set_font('NotoSans', 'B', 10) 
    except RuntimeError:
         if scale_factor < 0.7:
             pdf.set_font('Arial', 'B', 8)
         else:
             pdf.set_font('Arial', 'B', 10)

    for header in headers:
        # Trunca header se não couber mesmo após redimensionar de forma segura
        hdr_text = str(header)
        col_w = col_widths[header]
        while pdf.get_string_width(hdr_text) > col_w - 2 and len(hdr_text) > 0:
            hdr_text = hdr_text[:-1] # Remoção simples letra a letra para NUNCA gerar loop infinito
            
        pdf.cell(col_w, 8, hdr_text, 1, 0, 'C', 1) 
    pdf.ln()

    # Reset text color for body
    pdf.set_text_color(0, 0, 0)

    # --- FORMATAÇÃO DO CORPO ---
    try:
        if scale_factor < 0.7:
             pdf.set_font('NotoSans', '', 7)
        elif scale_factor < 0.85:
             pdf.set_font('NotoSans', '', 8)
        else:
             pdf.set_font('NotoSans', '', 9)
    except RuntimeError:
        if scale_factor < 0.7:
             pdf.set_font('Arial', '', 7)
        elif scale_factor < 0.85:
             pdf.set_font('Arial', '', 8)
        else:
             pdf.set_font('Arial', '', 9)

    pdf.set_fill_color(225, 235, 250) # Azul bem claro para as linhas alternadas (Zebra Blue)
    fill = False
    for _, row in df.iterrows():
        pdf.set_x(margin)
        fill = not fill
        for header in headers:
            if header in cols_single_checkbox:
                cell_text = "[  ]"
            elif header in cols_double_checkbox:
                cell_text = "[  ]   [  ]"
            else:
                cell_text = str(row.get(header, ''))
            
            col_width = col_widths.get(header, 10)
            
            # Truncador agressivo para corpo
            if pdf.get_string_width(cell_text) > col_width - 2: 
                while pdf.get_string_width(cell_text) > col_width - 2 and len(cell_text) > 0:
                    cell_text = cell_text[:-1]

            pdf.cell(col_width, 6, cell_text, 1, 0, 'L', fill) # Border 1 (com bordas)
        pdf.ln()
    
    # --- GERAÇÃO DE SAÍDA ROBUSTA (VIA ARQUIVO TEMPORÁRIO) ---
    temp_pdf_path = "temp_pdf_buffer.pdf"
    try:
        pdf.output(temp_pdf_path)
        with open(temp_pdf_path, "rb") as f:
            pdf_output_buffer = io.BytesIO(f.read())
        pdf_output_buffer.seek(0)
        return pdf_output_buffer
    except Exception as e:
        st.error(f"Falha ao gerar o buffer do PDF via arquivo temporário: {e}")
        return None
    finally:
        if os.path.exists(temp_pdf_path):
            os.remove(temp_pdf_path)