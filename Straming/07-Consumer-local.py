# Consumer 

import csv
import time
from google.cloud import pubsub_v1
import os

serviceAccount = r'C:\Users\rafae\OneDrive\Documentos\curso-dataflow-beam-469102-b318a3814cf9.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

subscription = 'projects/curso-dataflow-beam-469102/subscriptions/MeusVoos-sub'
subscriber = pubsub_v1.SubscriberClient()

def mostrar_msg(mensagem):
    print(('Mensagem: {}'.format(mensagem)))
    mensagem.ack()

subscriber.subscribe(subscription,callback=mostrar_msg)

while True:
    time.sleep(3)