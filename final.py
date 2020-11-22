from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from datetime import datetime
import time
import pymongo
from pymongo import MongoClient


## Processamento na camada batch para retornar os campos tempo e valor 

class tempo_e_valor(MRJob):
    def mapper(self, _, linha):

        campos = linha.split(",")

        data = campos[1]
        data1 = campos[2]


        dt_inicio = datetime.strptime(data, '%Y-%m-%d %H:%M:%S')
        dt_fim = datetime.strptime(data1, '%Y-%m-%d %H:%M:%S')

            # Convert to Unix timestamp
        d1_ts = time.mktime(dt_inicio.timetuple())
        d2_ts = time.mktime(dt_fim.timetuple())

        tempo_corrida =int(d2_ts-d1_ts) / 60

        total = campos[16]

        yield "resultado", {"tempo": tempo_corrida, "valor" :total}

        r = {
        	"data_inicio" : data,
        	"data_final" : data1,
        	"valor_total" : total,
        	"tempo_corrida": tempo_corrida
        }

        client = MongoClient("mongodb+srv://lucas:mongodb@cluster0.wqpq7.mongodb.net/totalcorridas?retryWrites=true&w=majority")
        db = client.get_database ('totalcorridas')
      
        cadastro = db.totalcorridas 
        cadastro.insert_one (r)


if __name__ == '__main__':
    tempo_e_valor.run()

# Map Reducer para definir o total por dia 

class total_cobranca(MRJob):
    def mapper(self, _, linha):
    	campos = linha.split(",")
    	datahora = campos[1].split(" ")
    	data = datahora[0]

    	total = float(campos[16])

    	yield data, total

    def reducer(self, chave, valores):

        yield chave, sum(valores)

if __name__ == '__main__':
    total_cobranca.run()

# Map reducer tempo medio de corrida em minutos 

class tempo_medio_dia(MRJob):
    def mapper(self, _, linha):

        campos = linha.split(",")

        datahora = campos[1].split(" ")
        dia = datahora[0]
        data = campos[1]
        data1 = campos[2]


        dt_inicio = datetime.strptime(data, '%Y-%m-%d %H:%M:%S')
        dt_fim = datetime.strptime(data1, '%Y-%m-%d %H:%M:%S')

            # Convert to Unix timestamp
        d1_ts = time.mktime(dt_inicio.timetuple())
        d2_ts = time.mktime(dt_fim.timetuple())

        tempo_corrida =int(d2_ts-d1_ts) / 60

        linha1 = 1

        #yield dia, {"tempo": float(tempo_corrida), "linha" :1}
        yield dia, tempo_corrida

    def reducer(self, chave, valores):
        lista = list(valores)
        total = sum(lista)
        qts = len(lista)

        media = total/qts
        yield chave, media

if __name__ == '__main__':
    tempo_medio_dia.run()


class valor_medio_15min(MRJob):
    def mapper(self, _, linha):
        campos = linha.split(",")

        data = campos[1]
        data1 = campos[2]
        hora = data1.split(" ")
        day = hora[0]
        h = hora[1].split(":")
        minuto = int(h[1])
        x = h[0]

        total = float(campos[16])

        if  (minuto <= 15) :  min2 = "00-15" 
        elif (minuto > 15 and minuto <= 30) : min2 = "15-30"
        elif (minuto > 30) and (minuto <= 45) : min2 = "30-45"
        else : min2 = "45-59"
        
        min3 = str(min2).split("-")
        intervalo = day + " " + x + ":" + min3[0] + " - " + x + ":" + min3[1] 

        yield intervalo, total

    def reducer(self, chave, valores):
        lista = list(valores)
        total = sum(lista)
        qts = len(lista)

        media = total/qts
        yield chave, media

if __name__ == '__main__':
    valor_medio_15min.run()








