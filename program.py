import pandas as pd
import sys
from datetime import datetime, timedelta
from AuxFuncoes.moduloConsultas import *
from AuxFuncoes.validacoes import *


if __name__ == "__main__":


    aux_data = True

    while aux_data:
        DATA_INICIAL = input('Entre com a data inicial no formato dd-mm-yyyy: ')
        DATA_FINAL = input('Entre com a data final no formato dd-mm-yyyy: ')

        if not validar_data(DATA_INICIAL):
            print("\nData inicial inválida! Use o formato dd-mm-yyyy.\n")
        elif not validar_data(DATA_FINAL):
            print("\nData final inválida! Use o formato dd-mm-yyyy.\n")
        else:
            aux_data = False
            print("\nDatas válidas!")

    print("\nCarregando...\n")
    data_inicial_dt = datetime.strptime(DATA_INICIAL, '%d-%m-%Y')
    data_final_dt = datetime.strptime(DATA_FINAL, '%d-%m-%Y')
    menor_data_inicial = (data_inicial_dt - timedelta(days=31)).strftime('%d/%m/%Y')
  
    DATA_INICIAL_ANIEL = datetime.strptime(DATA_INICIAL, '%d-%m-%Y').strftime('%Y-%m-%d')
    DATA_FINAL_ANIEL = datetime.strptime(DATA_FINAL, '%d-%m-%Y').strftime('%Y-%m-%d')

 
    os_validacao = retornaOsValidacao(DATA_INICIAL, DATA_FINAL)
    materiais_aniel = retornaMateriaisAniel(DATA_INICIAL_ANIEL, DATA_FINAL_ANIEL)


    colunas = [
        'Cidade', 'Data Exec.', 'NumOS', 'Cod. Ass.', 'Contrato', 'Nome Assinante',
        'Tipo', 'Nome Servico', 'Equipe', 'Emp', 'DOC.', 'Cod. Serv', 
        'OBS. CONTROLADORIA', 'OBS. MESMO DIA', 'VALOR OS',
        'OBS. END.', 'OBS. UP(DOWN)GRADE', 'OBS. OS'
    ]

    var = input("\nDeseja recriar base terceira, (S) ou (N): ")
    if var.lower().replace(" ", "") == "s":
        criar_base_terceiras(os_validacao, colunas)
    base = verificar_cidades_faltantes()
    os_up_down = criaStrUpdown(menor_data_inicial, data_final_dt)
    colocar_up_down(os_up_down)
    if base['OBS. CONTROLADORIA'].notnull().any():
        print("Existe cidades faltante")
        sys.exit()
    separar_terceiras()

    nome_planilha = 'DIGINET TECNOLOGIA DIVINOPOLIS'

    df_aux_garantias = pd.read_excel('AuxPlanilhas/AUX GARANTIA.xlsm', sheet_name='AUX Garantias')

    xls = pd.ExcelFile('planilhaS/BASE VALIDADA.xlsx')

    retiradas_do_corte = df_aux_garantias[df_aux_garantias['TIPO'] == 'RETIRADA CORTE']['COD.'].values.tolist()


    for aba in xls.sheet_names:
        if True:
            df = pd.read_excel(xls, sheet_name=aba)
            df = df.astype('object')

      
            df_aux = df.copy()

            df_aux['Data Exec.'] = pd.to_datetime(df_aux['Data Exec.'], dayfirst=True, errors='coerce')

            menor_data = df_aux['Data Exec.'].min()
            menor_data_inicial = (menor_data - timedelta(days=31)).strftime('%d/%m/%Y')
            maior_data = (df_aux['Data Exec.'].max()).strftime('%d/%m/%Y')
            
            dados_tipo_garantia = retornaOsGarantia(menor_data_inicial, maior_data, aba.split(" ")[0])
            dados_tipo_mesmo_endereco = retornaOsGarantia(DATA_INICIAL, DATA_FINAL, aba.split(" ")[0])
            for _, row in df_aux_garantias.iterrows():
                if row['COD.'] in df.loc[:, 'Cod. Serv'].values:
                    
                    tipo = row['TIPO'].strip()
                
                    if tipo == 'CONSUMO':
                        df = verificar_consumo(materiais_aniel,df, row['COD.'],row['TIPO MAT.'],row['OBS OK'], row['OBS NÃO'])
                    elif tipo == 'RETIRADA CORTE':
                        df = retirada_corte(materiais_aniel,df, row['COD.'],row['TIPO MAT.'],row['OBS OK'], row['OBS NÃO'])
                    elif tipo == 'GARANTIA':
                        df = garantia(dados_tipo_garantia, df_aux_garantias,df, row['COD.'],row['OBS OK'], row['OBS NÃO'])
                    elif tipo == 'NÃO PAGA':
                        df = nao_paga(df, row['COD.'], row['OBS NÃO'])
                    elif tipo == 'RETIRADA':
                        df = retirada(materiais_aniel,df, row['COD.'],row['TIPO MAT.'],row['OBS OK'], row['OBS NÃO'])
                    elif tipo == 'CORTE':
                        df = corte(df, row['COD.'],row['OBS OK'], row['OBS NÃO'], retiradas_do_corte)
                    elif tipo == 'SENHA':
                        df = troca_senha(df, row['COD.'], row['OBS OK'])
                    elif tipo == 'TRANSFERÊNCIA':
                        df = transferencia(materiais_aniel,df, row['COD.'],row['OBS OK'], row['OBS NÃO'])
                    elif tipo == 'TROCA':
                        df = troca(materiais_aniel,df, row['COD.'],row['TIPO MAT.'],row['OBS OK'], row['OBS NÃO'])
                    elif tipo == 'VERIFICAR':
                        df = verificar(df, row['COD.'])
            
            
            df = mesmo_dia(df)
            df = mesmo_endereco(df, dados_tipo_mesmo_endereco)
            df['Data Exec.'] = pd.to_datetime(df['Data Exec.'], dayfirst=True).dt.date
            df.to_excel('planilhaS/VALIDACAO ' + aba + '.xlsx', index=False)


