import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
   
    'project': 'curso-dataflow-beam-469102',
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_locations': 'gs://curso-apache-beam-123/temp',
    'temp_location': 'gs://curso-apache-beam-123/temp',
    'template_location': 'gs://curso-apache-beam-123/template/batch_job_df_gcs_big_query',
    'save_main_session': True

}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p6 = beam.Pipeline(options=pipeline_options)

serviceAccount = r'C:\Users\rafae\OneDrive\Documentos\curso-dataflow-beam-469102-b318a3814cf9.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

class filtro(beam.DoFn):
  def process(self,record):
      if int(record[8]) > 0:
        return [record]
      
def criar_dict_nivel1(record):
   dict_ = {}
   dict_['airport'] = record[0]
   dict_['lista'] = record[1]
   return(dict_)

def desaninhar_dict(record):
    def expand(key, value):
        if isinstance(value, dict):
            return [ (key + '_' + k, v) for k, v in desaninhar_dict(value).items() ]
        else:
            return [ (key, value) ]
    items = [ item for k, v in record.items() for item in expand(k, v) ]
    return dict(items)

def criar_dict_nivel0(record):
    dict_ = {} 
    dict_['airport'] = record['airport']
    dict_['lista_Qtd_Atrasos'] = record['lista_Qtd_Atrasos'][0]
    dict_['lista_Tempo_Atrasos'] = record['lista_Tempo_Atrasos'][0]
    return(dict_)

table_schema = 'airport:STRING, lista_Qtd_Atrasos:INTEGER, lista_Tempo_Atrasos:INTEGER'
tabela = 'curso-dataflow-beam-469102.curso_dataflow_123.curso_dataflow_voos_tb'

Tempo_Atrasos = (
    p6
         | "Importar Dados" >> beam.io.ReadFromText(r'gs://curso-apache-beam-123/entrada/voos_sample.csv', skip_header_lines = 1)
         | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
         | "Pegar voos com atraso" >> beam.ParDo(filtro())
         | "Criar par" >> beam.Map(lambda record: (record[4], int(record[8])))
         | "Somar por key" >> beam.CombinePerKey(sum)
)

Qtd_Atrasos = (
    p6
         | "Importar Dados 2" >> beam.io.ReadFromText(r'gs://curso-apache-beam-123/entrada/voos_sample.csv', skip_header_lines = 1)
         | "Separar por Vírgulas 2" >> beam.Map(lambda record: record.split(','))
         | "Pegar voos com atraso 2" >> beam.ParDo(filtro())
         | "Criar par 2" >> beam.Map(lambda record: (record[4], int(record[8])))
         | "Contar por key 2" >> beam.combiners.Count.PerKey()  
)

tabela_atrasos = (
    {'Qtd_Atrasos': Qtd_Atrasos, 'Tempo_Atrasos': Tempo_Atrasos}
    |"Group By" >> beam.CoGroupByKey()
    |"Lambda Nível 1" >> beam.Map(lambda record: criar_dict_nivel1(record))
    |"Desaninhar" >> beam.Map(lambda record: desaninhar_dict(record))
    |"Lambda Nível 0" >> beam.Map(lambda record: criar_dict_nivel0(record))
    |"Escrita BigQuery" >> beam.io.WriteToBigQuery(
                                                    tabela,
                                                    schema = table_schema,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    custom_gcs_temp_location='gs://curso-apache-beam-123/temp'
    )
)

p6.run()