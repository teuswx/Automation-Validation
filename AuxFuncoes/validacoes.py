import pandas as pd
import os
from datetime import datetime, timedelta
from queue import PriorityQueue
import re


def remover_caracteres_invalidos(valor):
    if isinstance(valor, str):
        return re.sub(r'[\x00-\x1F\x7F]', '', valor)
    return valor

def criar_base_terceiras(os_validacao, colunas):

    dados = [
        [remover_caracteres_invalidos(value) for value in item[:-1]] + [''] * (len(colunas) - len(item)) + [item[-1]]
        for item in os_validacao
    ]


    df = pd.DataFrame(dados, columns=colunas)
    dfa = pd.DataFrame()
    auxiliar_tecnologia = pd.read_excel("AuxPlanilhas/AUXILIAR TEC.xlsx")
    auxiliar_tecnologia['TECNOLOGIA'] = auxiliar_tecnologia['TECNOLOGIA'].str.lower().str.replace(" ", "")
    dfa['Tipo_norm'] = df['Tipo'].str.lower().str.replace(" ", "")

    mapeamento = dict(zip(auxiliar_tecnologia['TECNOLOGIA'], auxiliar_tecnologia['TIPO TEC.']))

    df['Tipo'] = dfa['Tipo_norm'].map(mapeamento).fillna(df['Tipo'])
    df['Data Exec.'] = pd.to_datetime(df['Data Exec.'], errors='coerce').dt.strftime('%d/%m/%Y')

    df.to_excel("planilhas/BASE TERCEIRAS.xlsx", sheet_name='BASE', index=False)

def colocar_up_down(os_up_down):
    df = pd.read_excel("planilhaS/BASE TERCEIRAS.xlsx")
    df["OBS. UP(DOWN)GRADE"] = df["OBS. UP(DOWN)GRADE"].astype("object")

    def gerar_obs(row, os_aux):
        os_data = datetime.strptime(row["Data Exec."], "%d/%m/%Y").date()
        diff_dias = int((os_data - os_aux[1]).days)
        
        if diff_dias == 0:
            return os_aux[7] + " MESMO DIA"
        elif diff_dias == 1:
            return  os_aux[7] +" 1 DIA ANTES"
        elif diff_dias > 1:
            return f" {os_aux[7]}  {diff_dias} DIAS ANTES"
        else:
            return f"{ os_aux[7]}  {-diff_dias} DIAS DEPOIS"

    for os_aux in os_up_down:
        condicao = (df["Cidade"] == os_aux[0]) & (df["Cod. Ass."] == os_aux[3]) & (df["Contrato"] == os_aux[4])
        df.loc[condicao, "OBS. UP(DOWN)GRADE"] = df[condicao].apply(lambda row: gerar_obs(row, os_aux), axis=1)

    
    df.to_excel("planilhaS/BASE TERCEIRAS.xlsx", sheet_name="BASE", index=False)
    
def separar_terceiras():
    base = pd.read_excel('planilhaS/BASE TERCEIRAS.xlsx', sheet_name='BASE')
    empreiteiras = pd.read_excel('AuxPlanilhas/AUX DADOS EMPREITEIRAS.xlsx', sheet_name='EMPREITEIRAS')

    base = base.astype('object')
    empreiteiras = empreiteiras.astype('object')

    os_por_emp = {}

    for _, row in empreiteiras.iterrows():
        dados_filtrados = base[(base["Emp"] == row["NUM EMP."]) & (base["Cidade"] == row["CIDADE"])]

        if not dados_filtrados.empty:
            aba = f"{row['EMPREITEIRA']} {row['REGIONAL']}"
            os_por_emp.setdefault(aba, []).append(dados_filtrados)

    
    pasta = "AuxPlanilhas/PLANILHAS DE VALORES"
    arquivos = [f for f in os.listdir(pasta) if f.endswith(('.xlsx', '.xls','.xlsm'))]
    
    with pd.ExcelWriter('planilhaS/BASE VALIDADA.xlsx', engine='xlsxwriter') as writer:
        for regional, dataframes in os_por_emp.items():
            
            df_regional = pd.concat(dataframes, ignore_index=True).drop(columns=['Emp'])

            codigo_valor = df_regional[['DOC.', 'Tipo', 'Cod. Serv']].apply(
                lambda x: '-'.join(x.astype(str)), axis=1).str.replace(" ", "")
            
            pasta = "AuxPlanilhas/PLANILHAS DE VALORES"
            
            arquivos = [f for f in os.listdir(pasta) if f.endswith(('.xlsx', '.xls', '.xlsm'))]
            
            for arquivo in arquivos:
                nome_valor = os.path.splitext(arquivo)[0].lower().split()
                nome_valor = nome_valor[1] + nome_valor[-1]
                
                nome_df_regional = regional.lower().split()
                nome_df_regional = nome_df_regional[0] + nome_df_regional[-1]
               
                if nome_valor == nome_df_regional:
                    caminho_arquivo = os.path.join(pasta, arquivo)
                    
                    df_valores = pd.read_excel(caminho_arquivo)
                    
                    df_valores['AUX'] = df_valores['AUX'].str.replace(" ", "", regex=False)
                    
                    valor_map = df_valores.set_index('AUX')['VALOR'].to_dict()
                    
                    df_regional['VALOR OS'] = codigo_valor.apply(
                        lambda x: valor_map.get(x, None)  
                    )

            nome_sheet = regional[:31]
            df_regional.to_excel(writer, sheet_name=nome_sheet, index=False)

def verificar_cidades_faltantes():

    aux_empreiteiras = pd.read_excel('AuxPlanilhas/AUX DADOS EMPREITEIRAS.xlsx', sheet_name='EMPREITEIRAS')
    base = pd.read_excel('planilhaS/BASE TERCEIRAS.xlsx')

    base = base.astype('object')

    cidades_por_emp_dict = aux_empreiteiras.groupby('NUM EMP.')['CIDADE'].apply(set).to_dict()

    base['OBS. CONTROLADORIA'] = None

    empresas_validas = base['Emp'].isin(cidades_por_emp_dict.keys())

    base.loc[~empresas_validas, 'OBS. CONTROLADORIA'] = '!EMPRESA NÃO ENCONTRADA EM AUX DADOS EMPREITEIRAS!'

    for num_emp, cidades_validas in cidades_por_emp_dict.items():
        base.loc[(base['Emp'] == num_emp) & (~base['Cidade'].isin(cidades_validas)), 'OBS. CONTROLADORIA'] = '!ESTA CIDADE NÃO EXISTE EM AUX DADOS EMPREITEIRAS!'

    base.to_excel('planilhaS/BASE TERCEIRAS.xlsx',sheet_name='BASE', index=False)
    return base



#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#AQUI ESTÃO PRESENTE AS FUNÇÕES QUE REALIZARAM A VALIDAÇÃO DE CADA OS


#CONSUMO
 
def verificar_consumo(materiais_aniel, df, cod, tipo_mat, obs_ok, obs_nao):

    lista_os = df[df['Cod. Serv'] == cod]
    tipo_mat=tipo_mat.strip()
    materiais_aniel_dict = {}

    if tipo_mat == 'ROTEADOR' or tipo_mat == 'TV BOX':
        materiais_aniel_dict = {int(mat[2]): mat[9] for mat in materiais_aniel if mat[13] != '-'.strip()}
    elif tipo_mat == 'MATERIAIS':
        materiais_aniel_dict = {int(mat[2]): mat[9] for mat in materiais_aniel}
    
    for idx, os in lista_os.iterrows():
        num_os = int(os['NumOS'])
        
        if num_os in materiais_aniel_dict:
            df.loc[idx, "OBS. CONTROLADORIA"] = obs_ok
        else:
            df.loc[idx, "OBS. CONTROLADORIA"] = obs_nao + ", SEM LANÇAMENTO DE MATERIAL CONSUMIDO"

    return df


#NÃO PAGA
 
def nao_paga(df, cod, obs_nao):

    df.loc[df['Cod. Serv'] == cod, "OBS. CONTROLADORIA"] = obs_nao

    return df

# RETIRADA

def retirada(materiais_aniel, df, cod, tipo_mat, obs_ok, obs_nao):
    lista_os = df[df['Cod. Serv'] == cod]
    tipo_mat=tipo_mat.strip()
    materiais_aniel_dict = {}
  
    if tipo_mat == 'ROTEADOR':
        materiais_aniel_dict = {int(mat[2]): mat[9] for mat in materiais_aniel if mat[10].lower().startswith("r") and 'roteador' in mat[9].strip().lower()}
    elif tipo_mat == 'TV BOX':
        materiais_aniel_dict = {int(mat[2]): mat[9] for mat in materiais_aniel if mat[10].lower().startswith("r") and 'tv box' in mat[9].strip().lower()}
    
    for idx, os in lista_os.iterrows():
        num_os = int(os['NumOS'])
        
        if num_os in materiais_aniel_dict:
            df.loc[idx, "OBS. CONTROLADORIA"] = obs_ok
        else:
            df.loc[idx, "OBS. CONTROLADORIA"] = obs_nao + ", SEM LANÇAMENTO DE MATERIAL RETIRADO"

    return df

def troca_senha(df, cod, obs_ok):
    df.loc[df['Cod. Serv'] == cod, "OBS. CONTROLADORIA"] = obs_ok
    return df

#TRANSFERÊNCIA 
def transferencia(materiais_aniel, df, cod, obs_ok, obs_nao):
    lista_os = df[df['Cod. Serv'] == cod].sort_values(by=['Nome Assinante'])

    contagem = lista_os['Cod. Ass.'].value_counts()
    cods_com_alerta = contagem[contagem != 2].index
    df.loc[df['Cod. Ass.'].isin(cods_com_alerta), 'OBS. CONTROLADORIA'] = 'ALERTA (VERIFICAR TRANSFERÊNCIA)'
    contagem_transferencia = lista_os[lista_os['Cod. Ass.'].isin(contagem[contagem == 2].index)][['Cod. Ass.', 'NumOS']].sort_values(by='NumOS')

    valores_unicos = set(int(os[2]) for os in materiais_aniel)

    for i in range(0, len(contagem_transferencia), 2):
        valores = contagem_transferencia.iloc[i:i+2]
        if int(valores.iloc[1]['NumOS']) in valores_unicos:
            df.loc[df['NumOS'] == valores.iloc[0]['NumOS'], 'OBS. CONTROLADORIA'] = f"{obs_nao}, COMPLEMENTAR DA OS {valores.iloc[1]['NumOS']}"
            df.loc[df['NumOS'] == valores.iloc[1]['NumOS'], 'OBS. CONTROLADORIA'] = obs_ok
        else:
            df.loc[df['NumOS'] == valores.iloc[0]['NumOS'], 'OBS. CONTROLADORIA'] = f"{obs_nao}, COMPLEMENTAR DA OS {valores.iloc[1]['NumOS']}"
            df.loc[df['NumOS'] == valores.iloc[1]['NumOS'], 'OBS. CONTROLADORIA'] = f"{obs_nao}, (ALERTA) VERIFICAR A OS SEM MATERIAL CONSUMIDO  "



    return df


#TROCA 
def troca(materiais_aniel, df, cod, tipo_mat, obs_ok, obs_nao):
    lista_os = df[df['Cod. Serv'] == cod]['NumOS']
    tipo_mat = tipo_mat.strip()
    
    for os in lista_os:
        consumo = False
        retirada = False
        for material in materiais_aniel:
            if int(os) == int(material[2]):
                if tipo_mat.lower() == 'materiais' or  tipo_mat.lower() == 'roteador':
                    if material[13] != '-'.strip():
                        consumo = True
                    if material[10].lower().strip().startswith("r") and material[13] != '-'.strip():
                        retirada = True
                elif tipo_mat.lower() == 'equipamento':
                    consumo = True
                    retirada = True
        
        if consumo and retirada:
            df.loc[df['NumOS'] == os, 'OBS. CONTROLADORIA'] = obs_ok
        elif consumo == True and retirada == False:
            df.loc[df['NumOS'] == os, 'OBS. CONTROLADORIA'] = obs_nao + ', SEM LANÇAMENTO DE MATERIAL RETIRADO'
        elif consumo == False and retirada == True:
            df.loc[df['NumOS'] == os, 'OBS. CONTROLADORIA'] = obs_nao + ', SEM LANÇAMENTO DE MATERIAL CONSUMIDO'
        elif consumo == False and retirada == False:
            df.loc[df['NumOS'] == os, 'OBS. CONTROLADORIA'] = obs_nao + ', SEM LANÇAMENTO DE MATERIAL CONSUMIDO E RETIRADO'
    
    return df


# VERIFICAR
def verificar(df, cod):
    df.loc[df['Cod. Serv'] == cod, "OBS. CONTROLADORIA"] = "ALERTA (VERIFICAR)"
    return df


# GARANTIA
def garantia(dados_tipo_garantia, df_garantias, df, cod, obs_ok, obs_nao):
    
    lista_os = df[df['Cod. Serv'] == cod].values.tolist()
    
    lista_garantia = (df_garantias[df_garantias['GARANTIA'] == 'S']['COD.']).values.tolist()
    for os in lista_os:
        os_data = datetime.strptime(os[1], "%d/%m/%Y").date()

        df.loc[df['NumOS'] == os[2], "OBS. CONTROLADORIA"] = obs_ok
        
        for garantia in dados_tipo_garantia:
            if os_data > garantia[1]  and garantia[1] > (os_data - timedelta(days=31)) and os[0] == garantia[0] and os[3] == garantia[3] and os[4] == garantia[4] and garantia[10] in lista_garantia:
                if int((os_data - garantia[1]).days) == 1:
                    df.loc[df['NumOS'] == os[2], "OBS. CONTROLADORIA"] = obs_nao + ",  " + str(garantia[7]) + " EM MENOS DE "+ str((os_data - garantia[1]).days)+ " DIA"
                else:
                    df.loc[df['NumOS'] == os[2], "OBS. CONTROLADORIA"] = obs_nao + ",  " + str(garantia[7]) + " EM MENOS DE "+ str((os_data - garantia[1]).days)+ " DIAS"

    return df


# RETIRADA CORTE
def retirada_corte(materiais_aniel, df, cod, tipo_mat, obs_ok, obs_nao):
    lista_os = df[df['Cod. Serv'] == cod]
    tipo_mat=tipo_mat.strip()
    materiais_aniel_dict = {}

    materiais_aniel_dict = {int(mat[2]): mat[9] for mat in materiais_aniel if (mat[13] != '-'.strip() or 'antena' in mat[9].lower()) and mat[10].strip().lower().startswith('r')}
    
    # Itera diretamente pelo DataFrame filtrado
    for idx, os in lista_os.iterrows():
        num_os = int(os['NumOS'])
        
        if num_os in materiais_aniel_dict:
            df.loc[idx, "OBS. CONTROLADORIA"] = obs_ok
        else:
            df.loc[idx, "OBS. CONTROLADORIA"] = obs_nao + ", SEM LANÇAMENTO DE MATERIAL RETIRADO"
    return df



def corte(df, cod, obs_ok, obs_nao, retiradas_do_corte):
    # Filtra os DataFrames sem criar cópias desnecessárias
    lista_os = df[df['Cod. Serv'] == cod]
    lista_os_retirada_corte = df[df['Cod. Serv'].isin(retiradas_do_corte)]

    def verificar_os(os):
        os_data_corte = datetime.strptime(os['Data Exec.'], "%d/%m/%Y").date()

        for _, retirada in lista_os_retirada_corte.iterrows():
            os_data_retirada = datetime.strptime(retirada['Data Exec.'], "%d/%m/%Y").date()

            if (os['Cidade'] == retirada['Cidade'] and 
                os['Cod. Ass.'] == retirada['Cod. Ass.'] and 
                os['Contrato'] == retirada['Contrato']):
                
                if os_data_corte > os_data_retirada:
                    return obs_ok
                elif os_data_corte <= os_data_retirada:
                    return f"{obs_nao} COMPLEMENTAR DA OS {retirada['NumOS']}"

        return obs_ok

    df.loc[df['Cod. Serv'] == cod, "OBS. CONTROLADORIA"] = lista_os.apply(verificar_os, axis=1)
    
    for _, retirada in lista_os_retirada_corte.iterrows():
        df.loc[df['NumOS'] == retirada['NumOS'], "OBS. CONTROLADORIA"] = 'NÃO PAGA A OS DE RETIRADA DE EQUIPAMENTO'

    return df

def mesmo_dia(df):

    contagem = df.groupby(['Cod. Ass.', 'Contrato', 'Cidade', 'Data Exec.']).size()
    
    grupos_repetidos = contagem[contagem > 1].reset_index()[['Cod. Ass.', 'Contrato', 'Cidade', 'Data Exec.']]
    
    df['OBS. MESMO DIA'] = ''

    for _, grupo in grupos_repetidos.iterrows():

        subset = df[
            (df['Cod. Ass.'] == grupo['Cod. Ass.']) &
            (df['Contrato'] == grupo['Contrato']) &
            (df['Cidade'] == grupo['Cidade']) &
            (df['Data Exec.'] == grupo['Data Exec.'])
        ]
        

        servicos = subset['Nome Servico'].tolist()
        
        for i, os_index in enumerate(subset.index):
            outros_servicos = [s for j, s in enumerate(servicos) if j != i]
            observacao = ', '.join(outros_servicos) + ' MESMO DIA' if outros_servicos else 'MESMO DIA'
            df.at[os_index, 'OBS. MESMO DIA'] = observacao

    df["OBS. MESMO DIA"] = df["OBS. MESMO DIA"].fillna("").astype(str)

    df_filtrado = df[(df['OBS. MESMO DIA'] != "") & (df['Cod. Serv'] != 2951) & (df['Cod. Serv'] != 1081)]
    df["OBS. MESMO DIA"] = df["OBS. MESMO DIA"].astype('object')

    lista_mesmo_dia = df_filtrado.values.tolist()
    
    pessoas = {}
    for os in lista_mesmo_dia:
        chave = f"{os[0]}{os[3]}".replace(" ", "").lower()
        if chave not in pessoas:
            pessoas[chave] = PriorityQueue()
        pessoas[chave].put((-os[13], os))
    
    lista_os = []
    for chave, fila in pessoas.items():
        for item in fila.queue:
           if pd.notna(item[1][11]):
            if 'ok a os ' in item[1][11].lower().strip():
                lista_os.append(item[1])
                break
    
    
    for os in lista_mesmo_dia:
        for os_aux in lista_os:
            if os[2] != os_aux[2] and os[0] == os_aux[0] and os[3] == os_aux[3] and 'ok a os '.lower().strip() in os[11].lower().strip():
                df.loc[df['NumOS'] == os[2], 'OBS. CONTROLADORIA'] = f"NÃO PAGA A OS DE {os[7].strip()}, {os_aux[7].strip()} MESMO DIA"
            elif os[2] != os_aux[2] and os[0] == os_aux[0] and os[3] == os_aux[3] and  not 'ok a os '.lower().strip() in os[11].lower().strip() and os[2]:
                df.loc[df['NumOS'] == os[2], 'OBS. CONTROLADORIA'] = f"{os[11].strip()} E {os_aux[7].strip()} MESMO DIA"
                
        
    return df


def mesmo_endereco(df, dados_tipo_mesmo_endereco):
    lista_os = df.values.tolist()
    
    lista_endereco = dados_tipo_mesmo_endereco

    lista_mesmo = set()
    for os in lista_endereco:
 
        for endereco in lista_endereco:
            if os[0] == endereco[0] and os[2] !=endereco[2] and os[3] == endereco[3] and os[4] != endereco[4] and os[11] == endereco[11] and (os[10]) != int(1081) and os[10] != 2951:
                lista_mesmo.add(os[2])
                break

    for os in lista_os:
        if os[2] in lista_mesmo:
            df.loc[df["NumOS"] == os[2], "OBS. END."] = "POSSÍVEL CLIENTE COM 2 CONTRATOS NO MESMO ENDEREÇO"
    return df