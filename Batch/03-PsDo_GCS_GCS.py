import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
   
    'project': 'curso-dataflow-beam-469102',
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_locations': 'gs://curso-apache-beam-123/temp',
    'temp_location': 'gs://curso-apache-beam-123/temp',
    'template_location': 'gs://curso-apache-beam-123/template/batch_job_df_gcs_voos'

}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p6 = beam.Pipeline(options=pipeline_options)

serviceAccount = r'C:\Users\rafae\OneDrive\Documentos\curso-dataflow-beam-469102-b318a3814cf9.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

class filtro(beam.DoFn):
  def process(self,record):
      if int(record[8]) > 0:
        return [record]

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
    |"Escrita GCP" >> beam.io.WriteToText(r"gs://curso-apache-beam-123/saida/voos_atrasados_qtd.csv")
)

p6.run()