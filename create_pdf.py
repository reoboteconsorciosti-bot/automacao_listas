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

    # --- LÓGICA DE LARGURA DE COLUNA REVISADA PARA MELHOR ESPAÇAMENTO ---
    col_widths = {}
    # Adicionadas larguras específicas para colunas da higienização
    TARGET_WIDTHS = {
        "Razao": 55,       # Aumentado de 45
        "Logradouro": 45,  # Aumentado de 38
        "Numero": 12,      
        "Bairro": 25,      # Aumentado de 22
        "Cidade": 22,      # Aumentado de 20
        "UF": 8,           
        "NOME": 63,        # Aumentado de 55
        "Whats": 29,       
        "CEL": 28,         # Ajustado para caber no limite
        "1º Contato": 22, "2º Contato": 22, "3º Contato": 22, 
        "Atend. Lig.(S/N)": 33, "Visita Marc.(S/N)": 33
    }
    fixed_width_cols = {h: TARGET_WIDTHS[h] for h in headers if h in TARGET_WIDTHS}
    total_fixed_width = sum(fixed_width_cols.values())
    variable_cols = [h for h in headers if h not in fixed_width_cols]
    num_variable_cols = len(variable_cols)
    
    remaining_width = page_width - total_fixed_width
    if num_variable_cols > 0:
        # Garante que a largura da coluna variável não seja negativa
        variable_col_width = max(10, remaining_width / num_variable_cols)
        for header in variable_cols:
            col_widths[header] = variable_col_width
    for header, width in fixed_width_cols.items():
        col_widths[header] = width
    # --- FIM DA LÓGICA DE LARGURA ---

    pdf.set_line_width(0.1) # Bordas mais finas
    pdf.set_x(margin)
    # Cabeçalho Azul Escuro com texto Branco (Estilo Premium)
    pdf.set_fill_color(22, 54, 92) # Azul escuro profissional
    pdf.set_text_color(255, 255, 255) # Texto branco no cabeçalho
    
    # --- FORMATAÇÃO DO CABEÇALHO ---
    try:
        pdf.set_font('NotoSans', 'B', 10) # Fonte maior e em negrito
    except RuntimeError:
        pdf.set_font('Arial', 'B', 10)

    for header in headers:
        # Border 1 no header para voltar as linhas de grade
        pdf.cell(col_widths.get(header, 10), 8, str(header), 1, 0, 'C', 1) 
    pdf.ln()

    # Reset text color for body
    pdf.set_text_color(0, 0, 0)

    # --- FORMATAÇÃO DO CORPO ---
    try:
        pdf.set_font('NotoSans', '', 9) # Fonte maior para o corpo
    except RuntimeError:
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
            if pdf.get_string_width(cell_text) > col_width - 4: # Mais margem
                while pdf.get_string_width(cell_text) > col_width - 4:
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