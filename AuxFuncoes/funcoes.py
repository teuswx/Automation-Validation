import pandas as pd 
from datetime import datetime

def retornaEmpreiteira():
    df = pd.read_excel('AuxPlanilhas/AUX DADOS EMPREITEIRAS.xlsx')
    empreiteiras = df['EMPREITEIRA'].dropna().unique()  

    valor = " OR ".join([f"f.nomerazao LIKE '{empresa}'" for empresa in empreiteiras])
    return valor

def retornaEmpreiteiraGarantia(nomeEmp):
    valor = retornaEmpreiteira()
    valor =  valor.split(" OR ")
    for emp in valor:
        if nomeEmp.lower() in emp.lower():
            return emp

def retornaTodasCidades():
    df = pd.read_excel("planilhas/BASE TERCEIRAS.xlsx")
    cidades = tuple(df["Cidade"].dropna().unique())
    return cidades

def  retornaCodUpDown():
    df = pd.read_excel("AuxPlanilhas/AUXILIAR TEC.xlsx", sheet_name='CODUPDOWN')
    codigos = df["CODUPDOWN"].dropna().unique()
    return tuple(map(int, codigos))

def validar_data(data_str):
    try:
        datetime.strptime(data_str, "%d-%m-%Y") 
        return True
    except ValueError:
        return False
