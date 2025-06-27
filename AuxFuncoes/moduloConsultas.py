from dotenv import load_dotenv
import psycopg2
import os
import fdb
import firebirdsql
from AuxFuncoes.funcoes import *

load_dotenv()

host = os.getenv("HOST_IMANAGER")
port = int(os.getenv("PORT_IMANAGER"))
database = os.getenv("DATABASE_IMANAGER")
user = os.getenv("USER_IMANAGER")
password = os.getenv("SENHA_BANCO")



def retornaOsValidacao(dataI, dataF):
    conn = None
    cursor = None
    try:
        
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()


        consulta = """SELECT 
                cid.nomedacidade AS "Cidade", 
                os2.d_dataexecucao AS "Data Exec.", 
                os2.numos AS "NumOS", 
                os2.codigoassinante AS "Cod. Ass.", 
                os2.codigocontrato AS "Contrato", 
                cl.nome AS "Nome Assinante", 
                STRING_AGG(DISTINCT tp.descricaotecnologia, ' - ' ORDER BY tp.descricaotecnologia ASC) AS "Tipo Tecnologia", 
                ls.descricaodoserv_lanc AS "Nome Servico", 
                CONCAT(f.nomerazao, ' - ', eq.nomedaequipe) AS "Equipe", 
                eq.codempreiteira AS "Emp", 
                CASE 
                    WHEN LENGTH(TRANSLATE(cl.cpf_cnpj, '/.-\\', '')) <= 11 THEN 'CPF' 
                    ELSE 'CNPJ' 
                END AS "DOC.", 
                os2.codservsolicitado AS "Cod. Serv",
                os2.observacoes as "Observacao"
            FROM ordemservico os2
            JOIN equipe eq ON eq.codigocidade = os2.cidade AND eq.codigodaequipe = os2.equipeexecutou 
            JOIN lanceservicos ls ON ls.codigodoserv_lanc = os2.codservsolicitado 
            JOIN clientes cl ON cl.codigocliente = os2.codigoassinante AND cl.cidade = os2.cidade 
            JOIN cont_prog cp ON cp.contrato = os2.codigocontrato AND cp.cidade = os2.cidade AND cp.codempresa = os2.codempresa 
            JOIN programacao pg ON pg.codigodaprogramacao = cp.protabelaprecos AND pg.codcidade = cp.cidade AND pg.tipoprogramacao IN (0, 1) 
            LEFT JOIN tipotecnologiapacote tp ON tp.codtipotecnologia = pg.codtipotecnologia 
            JOIN cidade cid ON cid.codigodacidade = os2.cidade 
            LEFT JOIN contratos ct ON ct.cidade = os2.cidade AND ct.contrato = os2.codigocontrato 
            LEFT JOIN fornecedor f ON f.codfornecedor = eq.codempreiteira 
            WHERE os2.d_dataexecucao BETWEEN %s AND %s
            AND ({})
            GROUP BY 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12,13
            ORDER BY 1, 2, 3
            """.format(retornaEmpreiteira())  

        cursor.execute(consulta, (dataI, dataF))
        resultados = cursor.fetchall()


        return resultados

    except Exception as e:
        print(f"Erro ao conectar ou executar comandos: {e}")

    finally:
       
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def retornaOsGarantia(dataI, dataF, nomeEmp):
    conn = None
    cursor = None
    try:
        
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()


        consulta = """SELECT
            cid.nomedacidade AS Cidade,
            os.d_dataexecucao AS "Data Exec.",
            os.numos AS NumOS,
            os.codigoassinante AS "Cod. Ass.",
            os.codigocontrato AS Contrato,
            cl.nome AS "Nome Assinante",
            STRING_AGG (DISTINCT(t.descricaotecnologia), ' - ' ORDER BY t.descricaotecnologia ASC) AS "Tipo Tec.",
            l.descricaodoserv_lanc AS "Nome Servico",
            CONCAT(f.nomerazao, ' - ', e.nomedaequipe) AS Equipe,
            CASE WHEN LENGTH(TRANSLATE(cl.cpf_cnpj,'/.-\','')) <=11 THEN 'CPF' ELSE 'CNPJ' END AS DOC,
            os.codservsolicitado AS "Cod. Serv",
            ct.enderecoconexao AS end
            FROM ordemservico os
            JOIN equipe e ON e.codigocidade=os.cidade AND e.codigodaequipe=os.equipeexecutou
            LEFT JOIN fornecedor f ON f.codfornecedor=e.codempreiteira
            JOIN lanceservicos l ON l.codigodoserv_lanc=os.codservsolicitado
            JOIN clientes cl ON cl.codigocliente=os.codigoassinante AND cl.cidade=os.cidade
            JOIN cont_prog cp ON cp.contrato=os.codigocontrato AND cp.cidade=os.cidade AND cp.codempresa=os.codempresa
            JOIN programacao p ON p.codigodaprogramacao=cp.protabelaprecos AND p.codcidade=cp.cidade AND p.tipoprogramacao IN (0,1)
            LEFT JOIN tipotecnologiapacote t ON t.codtipotecnologia=p.codtipotecnologia
            JOIN cidade cid ON cid.codigodacidade=os.cidade
            LEFT JOIN contratos ct ON ct.cidade=cid.codigodacidade AND ct.contrato=os.codigocontrato AND os.codigoassinante = ct.codigodocliente
            WHERE os.d_dataexecucao BETWEEN %s AND %s
            AND ({})
            GROUP BY 1,2,3,4,5,6,8,9,10,11,12
            ORDER BY 1,2,3
            """.format(retornaEmpreiteiraGarantia(nomeEmp))  

        cursor.execute(consulta, (dataI, dataF))
        resultados = cursor.fetchall()


        return resultados

    except Exception as e:
        print(f"Erro ao conectar ou executar comandos: {e}")

    finally:
        
        if cursor:
            cursor.close()
        if conn:
            conn.close()



def retornaMateriaisAniel(dataI,dataF):
    password = os.getenv("SENHA_BANCOANIEL")
    host = os.getenv("HOST_BANCOANIEL") 
    port = int(os.getenv("PORT_BANCOANIEL"))
    database = os.getenv("DATABASE_BANCOANIEL") 
    user = os.getenv("USER_BANCOANIEL")

    try:
        
        con = fdb.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        
        cursor = con.cursor()

        cursor.execute("""
        SELECT
        m.projeto as "Projeto",
        m.num_obra as "Num_OBRA",
        CASE 
            WHEN POSITION('/' IN m.num_obra) > 0 
            THEN SUBSTRING(m.num_obra FROM 1 FOR POSITION('/' IN m.num_obra) - 1)
            ELSE m.num_obra
        END AS "Num_OS",
        m.num_doc as "Cod_Contrato",
        m.titular as "Titular",
        ts.descricao as "Serviço",
        m.data_prod as "Data_Execução",
        m.hora_encerramento as "Hora_Encerramento",
        e.nome as "Técnico",
        p.descricao as "Material",
        mt.codmat as "Cod_Mat",
        mt.quantidade as "Quantidade Lancada",
        mt.quant_remov as "Quantidade Removida",
        mt.codcpl as "Cod_Complementar"
        FROM tb_documento_producao m
        join tb_equipe e on e.equipe = m.equipe
        join tb_tipo_servico_equipe ts on ts.cod_tipo_serv = m.cod_tipo_serv
        join tb_material_tarefa mt on mt.num_obra = m.num_obra
        join tb_material p on p.codmat = mt.codmat
        WHERE m.data_prod  >=?  and   m.data_prod <= ?;
            """,(dataI, dataF))
        rows = cursor.fetchall()

        con.close()
        return rows
    except firebirdsql.OperationalError as e:
        print("Erro de conexão ao banco de dados:", e)
    except Exception as e:
        print("Ocorreu um erro:", e)



def criaStrUpdown(dataI, dataF):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()

        consulta = """SELECT
            cid.nomedacidade AS Cidade,
            os.d_dataexecucao AS "Data Exec.",
            os.numos AS NumOS,
            os.codigoassinante AS "Cod. Ass.",
            os.codigocontrato AS Contrato,
            cl.nome AS "Nome Assinante",
            STRING_AGG(DISTINCT(t.descricaotecnologia), ' - ' ORDER BY t.descricaotecnologia ASC) AS "Tipo Tec.",
            l.descricaodoserv_lanc AS "Nome Servico",
            CONCAT(f.nomerazao, ' - ', e.nomedaequipe) AS Equipe,
            CASE WHEN LENGTH(TRANSLATE(cl.cpf_cnpj,'/.-\','')) <= 11 THEN 'CPF' ELSE 'CNPJ' END AS DOC,
            os.codservsolicitado AS "Cod. Serv"
            FROM ordemservico os
            JOIN equipe e ON e.codigocidade = os.cidade AND e.codigodaequipe = os.equipeexecutou
            LEFT JOIN fornecedor f ON f.codfornecedor = e.codempreiteira
            JOIN lanceservicos l ON l.codigodoserv_lanc = os.codservsolicitado
            JOIN clientes cl ON cl.codigocliente = os.codigoassinante AND cl.cidade = os.cidade
            JOIN cont_prog cp ON cp.contrato = os.codigocontrato AND cp.cidade = os.cidade AND cp.codempresa = os.codempresa
            JOIN programacao p ON p.codigodaprogramacao = cp.protabelaprecos AND p.codcidade = cp.cidade AND p.tipoprogramacao IN (0, 1)
            LEFT JOIN tipotecnologiapacote t ON t.codtipotecnologia = p.codtipotecnologia
            JOIN cidade cid ON cid.codigodacidade = os.cidade
            LEFT JOIN contratos ct ON ct.cidade = cid.codigodacidade AND ct.contrato = os.codigocontrato AND os.codigoassinante = ct.codigodocliente
            WHERE os.d_dataexecucao BETWEEN %s AND %s
            AND os.codservsolicitado IN %s
            AND cid.nomedacidade IN %s
            GROUP BY 1, 2, 3, 4, 5, 6, 8, 9, 10, 11
            ORDER BY 1, 2, 3
            """

        cursor.execute(consulta, (dataI, dataF, retornaCodUpDown(), retornaTodasCidades()))
        resultados = cursor.fetchall()

        return resultados

    except Exception as e:
        print(f"Erro ao conectar ou executar comandos: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()