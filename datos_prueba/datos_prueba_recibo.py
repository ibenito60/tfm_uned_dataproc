from dataclasses import dataclass
from unicodedata import name
import pandas as pd
from datetime import datetime, timedelta
import random
from faker import Faker
import numpy as np
import os

from pip import main



def generate_cuentas():
    """
    Generamos los datos para las cuentas 
    """
    fake = Faker('es_ES')

    origen_recibo = ['N','P','M']
    situcion_recibo = ['A','B']
    file = open("cuentas.txt", "w")
    #escribimos la cabecera
    file.write("idcuenta,idcliente,empresa,oficina,digcontrol,contrato,situacion,fecha_alta,fecha_baja,iban_total"+ os.linesep)
    for i in range(1,1000):
        iban = fake.iban()
        idempr = iban[4:8]
        idcent = iban[8:12]
        idprod = iban[12:15]
        idcontr = iban[15:]
        #fecha de baja
        situacion_baja = fake.random_element(situcion_recibo)
        fecha_alta = fake.date_between(datetime(2000,10,11),datetime(2022,7,6))
        if situacion_baja == 'A':
            fecha_baja = '9999-12-31'
        else:
            fecha_baja = fake.date_between(fecha_alta,datetime(2022,7,6))

        file.write(f'''{i},{i},{idempr},{idcent},{idprod},{idcontr},{situacion_baja},{fecha_alta},{fecha_baja},{iban}{os.linesep}''')



    file.close()
    

def generate_datos_cliente():
    fake = Faker('es_ES')

    file = open("clientes.txt","w")
    #enunciado
    file.write("idcliente,nombre,apellidos,fecnaci,direccion,numdni"+os.linesep)


    for i in range(1,1000):
        nombre = fake.first_name()
        apellidos = f'{fake.last_name()} {fake.last_name()}'
        fecha_nacimiento = fake.date_of_birth(None,15,100)
        direccion = fr'{fake.address()}'.replace('\n',' ').replace(',',';')
        dni = str.upper(fake.bothify('########?'))
        file.write(f'{i},{nombre},{apellidos},{fecha_nacimiento},{direccion},{dni}'+os.linesep)

    #cerramos el fichero
    file.close()     

def generate_datos_poliza():
    """
    Generamos los datos de las polizas
    """    
    fake = Faker('es_ES')
    file = open("polizas.txt","w")

    #definimos la cabecera
    file.write("numpoliza,pol_estado,fecha_efecto,fecha_sol,fecha_vencimiento,forma_pago,fecha_baja,causa_baja,fecha_suspension,causa_suspen"+os.linesep)

    estado_pol = ['PV','PA','PS']
    forma_pago = ['A','S','T','M','U']
    """
    falta pago
    fallecimiento
    peticion cliente
    traspaso
    """
    causa_baja = ['FP','FA','CL','TR']
    causa_suspen = ['FP','CL']
    #definimos el número de pólizas
    for i in range(1,2000):
        
        fecha_efecto = fake.date_between(datetime(2020,10,11),datetime(2022,7,6))
        fecha_baja = fake.date_between(fecha_efecto,datetime(2022,7,6))
        fecha_suspension = fake.date_between(fecha_efecto,datetime(2022,7,6))
        fecha_sol = fecha_efecto - timedelta(1)
        fecha_vencimiento = fake.date_between(fecha_efecto,datetime(2030,7,6))
        pol_estado = fake.random_element(estado_pol)
        forma_pago_2 = fake.random_element(forma_pago)
        caus_baja = fake.random_element(causa_baja)
        caus_suspen = fake.random_element(causa_suspen)

        if pol_estado == 'PV':
            fecha_baja = datetime(9999,12,31).date()
            fecha_suspension =datetime(9999,12,31).date()
            caus_baja = ' '
            caus_suspen = ' '
        elif pol_estado == 'PA':
            fecha_suspension = datetime(9999,12,31).date()
            caus_suspen = ' '
        elif pol_estado == 'PS':
            fecha_baja = datetime(9999,12,31).date()
            caus_baja = ' '
        
        #construimos el fichero
        file.write(f"{i},{pol_estado},{fecha_efecto},{fecha_sol},{fecha_vencimiento},{forma_pago_2},{fecha_baja},{caus_baja},{fecha_suspension},{caus_suspen}"+os.linesep)
    file.close()

def generate_datos_siniestro():
    """
    Generamos los datos del siniestro
    """

    fake = Faker('es_ES')
    #abrimos el fichero 
    file_sini = open('siniestros.txt','w')
    file_sini.write("idsiniestro,idpoliza,fecha_ocurs,fecha_apertura,fecha_cierre,tipo_siniestro,estado"+os.linesep)

    #file = open("siniestros.txt","w")
    estado_sin = ['AB','CE']
    tipo_siniestro = ['FA','IN','RE']
    #definimos la cabecera
    for i in range(1,4000):
        poliza = random.randrange(1,2000)
        fecha_ocurs = fake.date_between(datetime(2020,10,11),datetime(2022,7,6))
        fecha_apertura = fake.date_between(datetime(2020,10,11),datetime(2022,7,6))
        fecha_cierre = fake.date_between(fecha_apertura,datetime(2022,7,6))
        estado =fake.random_element(estado_sin)
        tipo_sin = fake.random_element(tipo_siniestro)
        if estado == 'AB':
            fecha_cierre = datetime(9999,12,31).date()
        file_sini.write(f'{i},{poliza},{fecha_ocurs},{fecha_apertura},{fecha_cierre},{tipo_sin},{estado}'+os.linesep)
    
    file_sini.close()

def generate_pagos_sini():
    """
    Generamos los pagos de los siniestros
    """

    fake = Faker('es_ES')
    file = open("pagos_siniestro.txt","w")

    #escribimos la cabecera
    file.write("idpago,idsiniestro,estado,fecha_pago,importe_pago,tipo_pago"+os.linesep)

    estado_pago = ['AN','CO']
    """
    transferencia
    efectivo
    """
    tipo_pago = ['T','E']

    for i in range(1,6000):
        idsiniestro = random.randrange(1,4000)
        fecha_pago = fake.date_between(datetime(2020,10,11),datetime(2022,7,6))
        estado =fake.random_element(estado_pago)
        importe_pago = random.randrange(100,100000)
        tipo_pag = fake.random_element(tipo_pago)
        if estado == 'AN':
            fecha_pago = datetime(9999,12,31).date()
        file.write(f'{i},{idsiniestro},{estado},{fecha_pago},{importe_pago},{tipo_pag}'+os.linesep)
    file.close()

def generate_tarifas():
    """
    Generamos los datos de las tarifas
    """

    fake = Faker('es_ES')
    file = open("tarifas.txt","w")

    #definimos la cabecera
    file.write("num_poliza,tarifa,situacion,fecha_alta,fecha_baja,importe_prima"+os.linesep)
    tarifas = ['TS1','TF1']
    situacion = ['A','B']
    for i in range(1,2000):
        tarifa1 = fake.random_element(tarifas)
        situa = fake.random_element(situacion)
        fecha_alta = fake.date_between(datetime(2020,10,11),datetime(2022,7,6))
        fecha_baja = fake.date_between(datetime(2020,10,11),datetime(2022,7,6))
        importe_prima = random.randrange(100,100000)
        if tarifa1 == 'TS1':
            tarifa2 = 'TF1'
        elif tarifa1 == 'TF1':
            tarifa2 = 'TS1'
        
        if situa == 'A':
            fecha_baja = datetime(9999,12,31).date()
        file.write(f'{i},{tarifa1},{situa},{fecha_alta},{fecha_baja},{importe_prima}'+os.linesep)
        file.write(f'{i},{tarifa2},{situa},{fecha_alta},{fecha_baja},{0}'+os.linesep)
    
    file.close()

def generate_recibos():
    """
    Generamos los datos de los recibos
    """
    fake = Faker('es_ES')

    #abrimos el fichero 
    file = open("recibos.txt","w")

    #definimos la cabecera
    file.write("idrecibo,fecha_emision,fecha_efecto,idpoliza,situacion,origen,importe_prima,importe_consorcio,importe_comision"+os.linesep)

    situac_rec = ['CO','AN']
    origen_rec = ['MA','AU']

    for i in range(1,8000):
        poliza = random.randrange(1,2000)
        fecha_efecto = fake.date_between(datetime(2020,10,11),datetime(2022,7,6))
        fecha_emision = fecha_efecto - timedelta(1)
        situacion = fake.random_element(situac_rec)
        origen = fake.random_element(origen_rec)
        importe_prima = random.randrange(100,100000)
        importe_consorcio = random.randrange(1,100)
        importe_comision = random.randrange(1,35)
        file.write(f"{i},{fecha_emision},{fecha_efecto},{poliza},{situacion},{origen},{importe_prima},{importe_comision},{importe_consorcio}"+ os.linesep)
    
    file.close()

def generate_intervinientes():
    """
    Generamos los datos para los intervinientes
    """
    fake = Faker('es_ES')

    file = open("intervinientes.txt","a")

    #definimos la cabecera
    #file.write("idcliente,numpoliza,tipo_intervencio,estado,fecha_alta,fecha_baja,orden_inter"+os.linesep)

    estado = ['A','B']
    tipo_inter = ['AS','TO']


    for i in range(1,1001):
        poliza = i + 1000 -1
        fecha_alta = fake.date_between(datetime(2020,10,11),datetime(2022,7,6))
        fecha_baja = fake.date_between(datetime(2020,10,11),datetime(2022,7,6))
        estado_inter = fake.random_element(estado)

        if estado_inter == 'B':
            fecha_baja = datetime(9999,12,31).date()

        file.write(f"{i},{poliza},AS,{estado_inter},{fecha_alta},{fecha_baja},1"+os.linesep)
    
    file.close()















if __name__ == "__main__":
    #generate_cuentas()
    #generate_datos_cliente()
    #generate_datos_poliza()
    #generate_datos_siniestro()
    #generate_pagos_sini()
    #generate_tarifas()
    #generate_intervinientes()
    generate_recibos()