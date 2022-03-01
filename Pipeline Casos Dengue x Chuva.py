import csv #Para persistir os arquivos em formato .CSV
import re #importando a biblioteca para uso de REGEX
import apache_beam as beam #importando e dando um alias
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText #Para realizar a leitura do arquivo

#Instanciando um objeto do tipo PipelineOptions
pipeline_options = PipelineOptions(argv =None)

#Criando um objeto chamado Pipeline
pipeline = beam.Pipeline(options = pipeline_options)

#Criando uma lista contendo as colunas do cabeçalho do dataset 'casos_dengue.txt'
colunas_dengue = [
                'id',
                'data_iniSE',
                'casos',
                'ibge_code',
                'cidade',
                'uf',
                'cep',
                'latitude',
                'longitude']

#Criando um método para receber uma string e colunas para converter em um dicionário
def lista_para_dicionario(string, colunas):

    """
    Recebe duas listas e retorna um dicionário
    """
    return dict(zip(colunas, string))

#Criando um método para receber uma string e dividí-la em uma lista
def texto_para_lista(string, delimitador = "|"):
    """
    Recebe um texto e um delimitador, retornando uma lista de elementos
    com base nesse delimitador (através do método split())
    """
    return string.split(delimitador)

#Criando um método para tratar as datas
def trata_datas(string): 
    """
    Recebe um dicionário e cria um novo campo com ANO-MES, retornando
    o mesmo dicionário com o novo campo
    """
    #Criando um novo campo chamado 'ano_mes', que tem como valor
    #   a divisão (split) do campo 'data_iniSE'. O split vai retornar a 
    #   data em formato string
    string['ano_mes'] = '-'.join(string['data_iniSE'].split('-')[:2])

    return string

#Criando um método para criar a tupla ESTADO-DICIONÁRIO, para que seja possível
#  agrupar posteriormente por Estado
def chave_uf(string):
    """
    Recebe um dicionário e retorna uma tupla com o estado (UF) e o elemento (UF, dicionario)
    """
    
    chave = string['uf']
    return (chave, string) #Retornando em forma de tupla

    #Exemplo:
    # ('CE', {'id': '33486', 'data_iniSE': '2019-01-20', ...})

def casos_dengue(string):
    """
    Recebe uma tupla('ESTADO', [{}, {}, ...] e retorna em uma tupla('ESTADO-DATA', qntd_casos)
    """

    uf, registros = string

    #Se colocasse um "return uf" no lugar do yield, ele iria retornar apenas um valor.
    #Com o "yield", ele irá retornar todos os valores conforme o loop
    #Com o f"{uf}-{registro['ano_mes']}, estou formatando (f) e colocando o valor de uf, seguido 
    #   de um traço, seguido do valor do registro['ano_mes']
    for registro in registros:
        #Caso retorne true, converte pra inteiro. Caso não, adiciona um 0 (isso para tratar as tuplas onde não há valor para 'casos',
        #    ou seja, para a quantidade de casos)
        #O "re.search(r'\d', registro['casos'])" é para verificar se a regex (r) tem um valor numérico (\d), no valor de registro['casos'] 
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos'])) #Retornando a tupla como '('ESTADO-DATA', qntd_casos)'
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0) #Retornando a tupla como '('ESTADO-DATA', qntd_casos)'

def chave_uf_ano_mes_de_lista(string):
    """
    Recebe uma lista de elementos
    e retorna uma tupla contendo uma chave e valor de chuva em mm ('UF-ANO-MES', valor)
    """
    #Descompactando o membro da lista em três variáveis (data, mm e uf, respectivamente)
    data, mm, uf = string

    ano_mes = '-'.join(data.split('-')[:2])

    chave = f'{uf}-{ano_mes}'
    
    #Tratando os casos onde o dataset tem mm como -9999 (pois não existe valor de chuva negativo)
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)

    return (chave, mm)

def arredondar(string):
    """
    Recebe uma tupla da forma ('UF-ANO-MES', XXXX.XXXXXX)
    e a retorna com o valor de mm de chuva arredondado ('UF-ANO-MES', XXXX.X)
    """
    chave, mm = string

    return (chave, round(mm, 2))

#Método para remover elementos que tenham chaves vazias
def filtra_campos_vazios(string):
    """
    Recebe uma tupla ('CE-2015-09', {'chuvas': [55.0], 'dengue': [680.0]})
    e retorna ela sem os campos que tenham chaves vazias
    """

    chave, dados = string

    #O "all()" está verificando se existe algum valor para 'dados['chuvas']' e para 'dados['dengue']
    if all([dados['chuvas'], dados['dengue']]):
       return True #returna True APENAS quando tem dado em 'chuvas' e em 'dengue'

    return False #retorna False quando não tem um ou nenhum dos dois


#Função para persistir os dados em arquivo
def converte_tupla(string):
    """Recebe uma tupla ('CE-2015-09', {'chuvas': [55.0], 'dengue': [680.0]}) e a retorna
    no formato ('CE-2015-09', 55.0, 680.0)"""

    chave, dados = string

    return f"{chave},{dados['chuvas'][0]},{dados['dengue'][0]}"

#Criando a pcollection
dengue = (
    
    pipeline
    #Para fins de teste, utilizar o arquivo 'sample_casos_dengue.txt'
    | "Leitura do dataset de dengue" >> ReadFromText('casos_dengue.txt', skip_header_lines=1)
    
    #Criando a etapa para poder chamar o método criado 'texto_para_lista'
    | "De texto para lista (pcollection dengue)" >> beam.Map(texto_para_lista)

    #Criando a etapa para transformar de lista em dicionário
    | "De lista para dicionário" >> beam.Map(lista_para_dicionario, colunas_dengue)

    #Criando a etapa para transformar a data de ANO-MES-DIA para ANO-MES
    | "Criar campo ano_mes" >> beam.Map(trata_datas)

    #Criando a etapa para criar uma chave para o estado (para ser usada na próxima etapa)
    | "Criar chave pelo estado" >> beam.Map(chave_uf)

    #Criando a etapa para agrupar com base no estado
    | "Agrupar pelo estado" >> beam.GroupByKey()

    #Criando a etapa para descompactar e gerar as tuplas no formato ('ESTADO-DATA', qntd_casos)
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)

    #Criando a etapa para somar os valores em que as chaves são iguais
    | "Soma dos casos pela chave" >> beam.CombinePerKey(sum)

    #Está comentado pois, se deixar descomentado, ele vai passar o resultado para o print, e não irá passar para a pcollection
    #   criada de 'resultado' (da linha 202 em diante)
#    | "Mostrar resultados" >> beam.Map(print)

)

#Pipeline referente ao dataset de chuvas
chuvas = (
    
    pipeline
    #Para fins de teste, utilizar o arquivo 'sample_chuvas.csv'
    | "Leitura do dataset de chuvas" >> ReadFromText('chuvas.csv', skip_header_lines = 1)

    | "De texto para lista (pcollection chuvas)" >> beam.Map(texto_para_lista, delimitador = ',')

    | "Criando a chave uf-ano-mes" >> beam.Map(chave_uf_ano_mes_de_lista)

    | "Soma do total de mm de chuva pela chave" >> beam.CombinePerKey(sum)

    | "Arredondar resultados de mm de chuva" >> beam.Map(arredondar)

    #Está comentado pois, se deixar descomentado, ele vai passar o resultado para o print, e não irá passar para a pcollection
    #   criada de 'resultado'
#   | "Mostrar resultado de chuvas" >> beam.Map(print)

)

#Criando uma pcollection "resultado" para poder juntar os resultados das pcollection de 'chuvas' e de 'dengues', com base na CHAVE
resultado = (
    
    #Passando a tupla como um dicionário, para indicar quem é o 'chuvas' e quem é o 'dengue'
    ({'chuvas': chuvas, 'dengue': dengue})

    #Usando o método 'beam.CoGroupByKey()'
    | "Mesclar pcollections" >> beam.CoGroupByKey()

    #Etapa para filtragem de resultados vazios
    #O "beam.Filter()" verifica se está ou não vazio, para manter ou tirar (ele faz essa "validação" com base em um retorno de
    #    True ou False)
    | "Filtrar dados vazios" >> beam.Filter(filtra_campos_vazios)

    #Como uma forma de tratamento no Beam (caso desejado utilizar), essa etapa realiza a conversão da tupla em string (verificar
    #   exemplo na documentação da função 'converte_tupla')
    | "Convertendo em lista" >> beam.Map(converte_tupla)

    #Está comentado pois, se deixar descomentado, ele vai passar o resultado para o print, e não irá passar para a pcollection
    #   criada de 'resultado'
    #| "Mostrar resultado da união" >> beam.Map(print)

    #Persistindo os dados em disco
    | "Persistir dados em arquivo" >> beam.io.WriteToText('dados_dengue_chuva', file_name_suffix = '.txt', header = "chave, chuvas, dengue", footer = None)
)

#Comando para que a pipeline possa rodar
pipeline.run()