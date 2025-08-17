# pip install google-cloud-pubsub
#producer

import csv
import time
from google.cloud import pubsub_v1
import os

serviceAccount = r'C:\Users\rafae\OneDrive\Documentos\curso-dataflow-beam-469102-b318a3814cf9.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

topico = 'projects/curso-dataflow-beam-469102/topics/MeusVoos'
publisher = pubsub_v1.PublisherClient()

entrada = r'C:\Users\rafae\OneDrive\Documentos\Flights-Dataflow\voos_sample.csv'

with open(entrada, 'rb') as file:
    for row in file:
        print('Publishing in Topic')
        publisher.publish(topico, row)
        time.sleep(2)