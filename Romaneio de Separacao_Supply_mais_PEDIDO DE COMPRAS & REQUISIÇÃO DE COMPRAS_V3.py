# Importação das bibliotecas # ROMANEIO DE SEPARAÇÃO SUPPLY 
import pandas as pd
import os
import datetime
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

#CÓDIGO DO ITEM'
# Leitura dos arquivos Excel
df1 = pd.read_excel(r"C:\Users\sar8577\Documents\AA_DATA FRAME_PYTHON\df_romaneio_supply_aprovado.xlsx")
df2 = pd.read_excel(r"C:\Users\sar8577\Documents\AA_DATA FRAME_PYTHON\df_listagemTurmas.xlsx")
df3 = pd.read_excel(r"C:\Users\sar8577\Documents\AA_DATA FRAME_PYTHON\df_posicao_financeira_25.xlsx") # TEM QUE ATUALIZAR A POSIÇÃO FINANCEIRA POR CAUSA DO ESTOQUE DA FILIAL 01
df4 = pd.read_excel(r"C:\Users\sar8577\Documents\AA_DATA FRAME_PYTHON\df_pedidos_de_compras_II_MXM_Filial_25.xlsx")
df5 = pd.read_excel(r"C:\Users\sar8577\Documents\AA_DATA FRAME_PYTHON\Requisições de compras_CD_PENDENTES DE PC.xlsx")

# Renomear colunas em df4
df4.rename(columns={'Unnamed: 38': 'SALDO DO PEDIDO DE COMPRAS', 'Unnamed: 21': 'ITEM'}, inplace=True)

# Aplicar filtros em df4
df4_filtrado = df4[
    (df4['Unnamed: 0'] == "025  -  CENTRO DE DISTRIBUIÇÃO SENAC") &
    (df4['Unnamed: 36'].isin(["Aprovado", "Atend. Parcial"])) &
    (df4['Unnamed: 39'].isin(["Sem Classificação", "Dispensa", "Inexigível"]))
]

# Informações
print(f"Romaneio possui {df1.shape[0]} linhas e {df1.shape[1]} colunas.")
print(f"Listagem Turmas possui {df2.shape[0]} linhas e {df2.shape[1]} colunas.")
print(f"Posição Financeira possui {df3.shape[0]} linhas e {df3.shape[1]} colunas.")
print(f"Pedido de Compras possui {df4_filtrado.shape[0]} linhas e {df4_filtrado.shape[1]} colunas.")
print(f"Requisições de Compras Pendente possui {df5.shape[0]} linhas e {df5.shape[1]} colunas.")

# Limpeza e preparação dos dados
df2 = df2.iloc[21:]
df3 = df3.iloc[9:]

# Ordenar do maior para o menor número de turmas
df2 = df2.sort_values(by=['Unnamed: 0'], ascending=False)

# Criar novas colunas de identificação de projetos
df1['Num_Projeto_Romaneio'] = df1['PROJETO'].str[:13]
df2['Num_Projeto_Listagem_Turmas'] = df2['Unnamed: 0'].str[:13]

# Agrupamento de df2 por 'Num_Projeto_Listagem_Turmas'
df2_grouped = df2.groupby('Num_Projeto_Listagem_Turmas').agg({
    'Unnamed: 21': 'first',
    'Unnamed: 28': 'first',
    'Unnamed: 31': 'first'
})

# Mapear informações do agrupamento para df1
df1['Status da Turma'] = df1['Num_Projeto_Romaneio'].map(df2_grouped['Unnamed: 21'])
df1['Inicio da Turma'] = df1['Num_Projeto_Romaneio'].map(df2_grouped['Unnamed: 28'])
df1['Termino da Turma'] = df1['Num_Projeto_Romaneio'].map(df2_grouped['Unnamed: 31'])



# Adição do prefixo 'Estágio' ao status da turma
df1['Status da Turma'] = df1.apply(
    lambda row: f"Estágio - {row['Status da Turma']}" if row['TIPO DE SOLICITAÇÃO'] == 'EST' else row['Status da Turma'],
    axis=1
)

# Converter todas as colunas de texto para letras maiúsculas
df1 = df1.apply(lambda col: col.str.upper() if col.dtype == 'object' else col)

# Adicionar a coluna 'ESTOQUE ATUAL CD' usando dados do df3
df1 = df1.merge(df3[['Posição financeira do estoque', 'Unnamed: 6']], how='left', left_on='CÓD DO ITEM', right_on='Posição financeira do estoque')
df1.rename(columns={'Unnamed: 6': 'ESTOQUE ATUAL CD'}, inplace=True)
df1.drop(columns=['Posição financeira do estoque'], inplace=True)

# Substituir valores ausentes em 'ESTOQUE ATUAL CD' por 0
df1['ESTOQUE ATUAL CD'] = df1['ESTOQUE ATUAL CD'].fillna(0)

# Criar a coluna 'SALDO DO PEDIDO DE COMPRAS' somando a quantidade do df4 filtrado
df4_soma = df4_filtrado.groupby('ITEM')['SALDO DO PEDIDO DE COMPRAS'].sum()
df1['SALDO DO PEDIDO DE COMPRAS'] = df1['CÓD DO ITEM'].map(df4_soma).fillna(0)

# Adicionar a coluna 'SALDO DAS RC's PENDENTES DE PC' usando dados do df5
df5_soma = df5.groupby('ITEM')['QUANTIDADE PENDENTE'].sum()
df1['SALDO DAS RC\'s PENDENTES DE PC'] = df1['CÓD DO ITEM'].map(df5_soma).fillna(0)

# Ordenar df1 por 'DATA DE ENTREGA' e 'Nº DA REQ.'
df1 = df1.sort_values(by=['DATA DE ENTREGA', 'Nº DA REQ.'])

# Inicializar variáveis para controle de saldo
estoque_saldo = df1.groupby('CÓD DO ITEM')['ESTOQUE ATUAL CD'].first().to_dict()
df1['ESTOQUE SEPARAR NOVO'] = 0
df1['SALDO ESTOQUE SEPARAR'] = 0
df1['NOVO STATUS DE SEPARAÇÃO'] = ''

# Loop para cálculo de empenho de estoque
for idx, row in df1.iterrows():
    item_codigo = row['CÓD DO ITEM']
    qtde_solicitada = row['QTDE SOLICITADA CD']
    saldo_atual = estoque_saldo.get(item_codigo, 0)

    # Cálculo do empenho
    if saldo_atual >= qtde_solicitada:
        df1.at[idx, 'ESTOQUE SEPARAR NOVO'] = qtde_solicitada
        estoque_saldo[item_codigo] -= qtde_solicitada
        df1.at[idx, 'SALDO ESTOQUE SEPARAR'] = estoque_saldo[item_codigo]
        df1.at[idx, 'NOVO STATUS DE SEPARAÇÃO'] = 'ESTOQUE SEPARAR'
    elif saldo_atual > 0:
        df1.at[idx, 'ESTOQUE SEPARAR NOVO'] = saldo_atual
        df1.at[idx, 'SALDO ESTOQUE SEPARAR'] = saldo_atual - qtde_solicitada
        estoque_saldo[item_codigo] = 0
        df1.at[idx, 'NOVO STATUS DE SEPARAÇÃO'] = 'SALDO ESTOQUE SEPARAR PARCIAL'
    else:
        df1.at[idx, 'ESTOQUE SEPARAR NOVO'] = 0
        df1.at[idx, 'SALDO ESTOQUE SEPARAR'] = saldo_atual - qtde_solicitada
        df1.at[idx, 'NOVO STATUS DE SEPARAÇÃO'] = 'NÃO SEPARAR'
        
        # Criar nova coluna 'SALDO DE PEDIDOS SEPARAR' para itens com SALDO ESTOQUE SEPARAR < 0  
saldo_pc = df1.groupby('CÓD DO ITEM')['SALDO DO PEDIDO DE COMPRAS'].first().to_dict()
df1['SALDO DE PEDIDOS SEPARAR'] = 0

for idx, row in df1.iterrows():
    item_codigo = row['CÓD DO ITEM']
    saldo_separar = row['SALDO ESTOQUE SEPARAR']
    baixado_saldo = saldo_pc.get(item_codigo, 0)

    # Empenho para pedidos baixados
    if saldo_separar < 0 and baixado_saldo > 0:
        if abs(saldo_separar) <= baixado_saldo:
            df1.at[idx, 'SALDO DE PEDIDOS SEPARAR'] = abs(saldo_separar)
            saldo_pc[item_codigo] -= abs(saldo_separar)
        else:
            df1.at[idx, 'SALDO DE PEDIDOS SEPARAR'] = baixado_saldo
            saldo_pc[item_codigo] = 0

# Adicionar a coluna "STATUS DE SEPARAÇÃO PC" com lógica baseada em "SALDO DE PEDIDOS SEPARAR"
df1['STATUS DE SEPARAÇÃO PC'] = ''

for idx, row in df1.iterrows():
    saldo_pedidos_separar = row['SALDO DE PEDIDOS SEPARAR']
    qtde_solicitada = row['QTDE SOLICITADA CD']
    
    # Determinação do "STATUS DE SEPARAÇÃO PC"
    if saldo_pedidos_separar >= qtde_solicitada:
        df1.at[idx, 'STATUS DE SEPARAÇÃO PC'] = 'SALDO PC ATENDE'
    elif saldo_pedidos_separar > 0:
        df1.at[idx, 'STATUS DE SEPARAÇÃO PC'] = 'SALDO PC ATENDE PARCIAL'
    else:
        df1.at[idx, 'STATUS DE SEPARAÇÃO PC'] = 'SALDO PC NÃO ATENDE'

# Adicionar coluna 'SALDO DE RC SEPARAR' com lógica semelhante a 'SALDO DE PEDIDOS SEPARAR'
saldo_rc = df1.groupby('CÓD DO ITEM')['SALDO DAS RC\'s PENDENTES DE PC'].first().to_dict()
df1['SALDO DE RC SEPARAR'] = 0

for idx, row in df1.iterrows():
    item_codigo = row['CÓD DO ITEM']
    saldo_separar = row['SALDO ESTOQUE SEPARAR']
    saldo_pendente_rc = saldo_rc.get(item_codigo, 0)

    # Empenho para RCs pendentes
    if saldo_separar < 0 and saldo_pendente_rc > 0:
        if abs(saldo_separar) <= saldo_pendente_rc:
            df1.at[idx, 'SALDO DE RC SEPARAR'] = abs(saldo_separar)
            saldo_rc[item_codigo] -= abs(saldo_separar)
        else:
            df1.at[idx, 'SALDO DE RC SEPARAR'] = saldo_pendente_rc
            saldo_rc[item_codigo] = 0

# Definir status de separação para as RCs pendentes
df1['STATUS DE SEPARAÇÃO RC'] = df1.apply(
    lambda row: 'SALDO RC ATENDE' if row['SALDO DE RC SEPARAR'] >= row['QTDE SOLICITADA CD'] else
    'SALDO RC ATENDE PARCIAL' if 0 < row['SALDO DE RC SEPARAR'] < row['QTDE SOLICITADA CD'] else
    'SALDO RC NÃO ATENDE', axis=1
)

# Cálculo do valor total
df1['VALOR TOTAL'] = df1['VALOR UNIT.'] * df1['QTDE SOLICITADA CD']
df1['VALOR TOTAL SEPARAR'] = df1['VALOR UNIT.'] * df1['ESTOQUE SEPARAR NOVO']

nova_ordem= ['Nº DA REQ.CÓD DO ITEM',	'TIPO ROMANEIO',	'Num_Projeto_Romaneio',	'Status da Turma',	'Inicio da Turma',	
             'Termino da Turma',	'COD FILIAL',	'FILIAL',	'Nº DA REQ.',	'SEQ.',	'TIPO DE SOLICITAÇÃO',	'C CUSTO',	
             'PROJETO',	'STATUS DO PROJETO',	'DATA DE EMISSÃO',	'DATA DE ENTREGA',	'MATRÍCULA DO REQ.',	'STATUS DA REQ.',	
             'OBS.',	'JUSTIFICATIVA',	'GRUPO DE COTAÇÃO',	'CÓD DO ITEM',	'DESCRIÇÃO',	'UNID.','QTDE SOLICITADA CD',	'VALOR UNIT.',	'VALOR TOTAL',	
             'ESTOQUE ATUAL CD',	'ESTOQUE SEPARAR NOVO',	'VALOR TOTAL SEPARAR', 'NOVO STATUS DE SEPARAÇÃO',	'SALDO ESTOQUE SEPARAR','SALDO DO PEDIDO DE COMPRAS',	
             'SALDO DE PEDIDOS SEPARAR',	'STATUS DE SEPARAÇÃO PC',	"SALDO DAS RC's PENDENTES DE PC",	'SALDO DE RC SEPARAR',	'STATUS DE SEPARAÇÃO RC',
]
df1 = df1[nova_ordem]

# Exportar para Excel
formato_data_hora = "%Y%m%d_%H%M%S"
data_hora_atual = datetime.datetime.now().strftime(formato_data_hora)
nome_arquivo = f"Romaneio_Supply_Aprovado_x_Status_da_Turma_+Saldo PC e RC{data_hora_atual}.xlsx"
caminho_saida = "C:/Users/sar8577/Downloads/"
df1.to_excel(caminho_saida + nome_arquivo, index=False)

print(df1.head(10))
print('Romaneio Supply Aprovado MAIS PEDIDOS DE COMPRAS !!!')