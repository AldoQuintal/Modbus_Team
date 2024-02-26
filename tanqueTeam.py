#!/usr/bin/env python3
""" Pymodbus Comunicacion con Equipos de Monitoreo de Tanques TEAM.

Se comunica a traves de un convertidor USB-485
en modo RTU a 38400

"""
import logging
import os
import psycopg2
import time
from datetime import date
from datetime import datetime
import requests
import json

# --------------------------------------------------------------------------- #
# import the various client implementations
# --------------------------------------------------------------------------- #
from pymodbus.client import (
    ModbusSerialClient,
    ModbusTcpClient,
    ModbusTlsClient,
    ModbusUdpClient,
)
from pymodbus.framer.rtu_framer import ModbusRtuFramer
from numpy import *

_logger = logging.getLogger()

SLAVE = 0x01

#  Configurar la conexion a PostgresSQL
PSQL_HOST = "localhost"
PSQL_PORT = "5432"
PSQL_USER = "postgres"
PSQL_PASS = "abcd1234"
PSQL_DB   = "TanquesTeam"

# Vamos a poner de manera global el numero de tanque escaneado
glb_tanque = '00'

def FSM_Core():
    while( True ):

        
        ProcesaInventario()
        print("Esperando 30 seg para consultar sensores")

        time.sleep(30)



def _handle_input_registers(client):
    global glb_tanque
    global tiempo_generar_cv

    """ 04 Read input registers."""
    _logger.info("### read input registers")

    # Antes de pedir la informacion por comunicaciones debemos ver cuales son los registros requeridos
    Tank_matrix = dict([
        ('01', dict([('inicio', 8), ('len', 6)])), #Rergistro 1 Led 8 para sensores
        ('02', dict([('inicio', 14), ('len', 6)])),
        ('03', dict([('inicio', 20), ('len', 6)])),
        ('04', dict([('inicio', 26), ('len', 6)])),
        ('05', dict([('inicio', 32), ('len', 6)])),
        ('06', dict([('inicio', 38), ('len', 6)])),
        ('07', dict([('inicio', 44), ('len', 6)])),
        ('08', dict([('inicio', 50), ('len', 6)])),
    ])

    print(f' tank_matrix_ {Tank_matrix}')
    tankid_global = '01'
    print(f'tank_global: {tankid_global}')

    # Recupera los valores a utilizar
    inicio = Tank_matrix[tankid_global]['inicio']
    print(f'Inicio: {inicio}')
    longitud = Tank_matrix[tankid_global]['len']
    print(f'Logitud: {longitud}')

    # Unicamente pediremos la informacion del tanque correspondiente
    print(f"Solicitando por modbus el Tanque {tankid_global}, desde el registro {inicio}, con longitud {longitud}")

    try:
        rr = client.read_input_registers(inicio, longitud, slave=SLAVE)
        print("____________________________________________________________")
        print(rr)
       # assert not rr.isError()
        if rr.isError():
            return

    except IOError as e:
        print("I/O error ({0}): {1}".format(e.errno, e.strerr))
        return

    # Una vez leido los registros vamos a interpretarlos
    # El equipo de monitoreo soporta 8 tanques
    # tiene 6 registros para 3 campos
    #tanques = zeros((8, 6))
    tanques = zeros((1, 6))

    # Vamos a recuperar la informacion de todos los tanques disponibles
    #idx = 0
    #for idx_tqs in range(len(tanques)):
    #    tanques[idx_tqs][0] = rr.registers[idx]
    #    tanques[idx_tqs][1] = rr.registers[idx+1]
    #    tanques[idx_tqs][2] = rr.registers[idx+2]
    #    tanques[idx_tqs][3] = rr.registers[idx+3]
    #    tanques[idx_tqs][4] = rr.registers[idx+4]
    #    tanques[idx_tqs][5] = rr.registers[idx+5]
    #    idx = idx + 6

    idx = 0
    tanques[0][0] = rr.registers[idx]
    tanques[0][1] = rr.registers[idx+1]
    tanques[0][2] = rr.registers[idx+2]
    tanques[0][3] = rr.registers[idx+3]
    tanques[0][4] = rr.registers[idx+4]
    tanques[0][5] = rr.registers[idx+5]

    # Vamos a ver que informacion nos trae el equipo de monitoreo
    print(tanques)

    # Una vez que tenemos los registros identificados hay que recuperar los valores correspondientes
    # Vamos a crear un diccionario para mayor facilidad, ajustandonos al criterio de que un registro
    # tiene el valor entero y el siguiente tiene el valor en milesimas de la cantidad
    #Tqs = dict([
    #    ('01', dict([('nivel', float(str(int(tanques[0][0])) + '.' + str(int(tanques[0][1])).zfill(3))),
    #                 ('agua', float(str(int(tanques[0][2])) + '.' + str(int(tanques[0][3])).zfill(3))),
    #                 ('temp', float(str(int(tanques[0][4])) + '.' + str(int(tanques[0][5])).zfill(3)))])),
    #    ('02', dict([('nivel', float(str(int(tanques[1][0])) + '.' + str(int(tanques[1][1])).zfill(3))),
    #                 ('agua', float(str(int(tanques[1][2])) + '.' + str(int(tanques[1][3])).zfill(3))),
    #                 ('temp', float(str(int(tanques[1][4])) + '.' + str(int(tanques[1][5])).zfill(3)))])),
    #    ('03', dict([('nivel', float(str(int(tanques[2][0])) + '.' + str(int(tanques[2][1])).zfill(3))),
    #                 ('agua', float(str(int(tanques[2][2])) + '.' + str(int(tanques[2][3])).zfill(3))),
    #                 ('temp', float(str(int(tanques[2][4])) + '.' + str(int(tanques[2][5])).zfill(3)))])),
    #    ('04', dict([('nivel', float(str(int(tanques[3][0])) + '.' + str(int(tanques[3][1])).zfill(3))),
    #                 ('agua', float(str(int(tanques[3][2])) + '.' + str(int(tanques[3][3])).zfill(3))),
    #                 ('temp', float(str(int(tanques[3][4])) + '.' + str(int(tanques[3][5])).zfill(3)))])),
    #    ('05', dict([('nivel', float(str(int(tanques[4][0])) + '.' + str(int(tanques[4][1])).zfill(3))),
    #                 ('agua', float(str(int(tanques[4][2])) + '.' + str(int(tanques[4][3])).zfill(3))),
    #                 ('temp', float(str(int(tanques[4][4])) + '.' + str(int(tanques[4][5])).zfill(3)))])),
    #    ('06', dict([('nivel', float(str(int(tanques[5][0])) + '.' + str(int(tanques[5][1])).zfill(3))),
    #                 ('agua', float(str(int(tanques[5][2])) + '.' + str(int(tanques[5][3])).zfill(3))),
    #                 ('temp', float(str(int(tanques[5][4])) + '.' + str(int(tanques[5][5])).zfill(3)))])),
    #    ('07', dict([('nivel', float(str(int(tanques[6][0])) + '.' + str(int(tanques[6][1])).zfill(3))),
    #                 ('agua', float(str(int(tanques[6][2])) + '.' + str(int(tanques[6][3])).zfill(3))),
    #                 ('temp', float(str(int(tanques[6][4])) + '.' + str(int(tanques[6][5])).zfill(3)))])),
    #    ('08', dict([('nivel', float(str(int(tanques[7][0])) + '.' + str(int(tanques[7][1])).zfill(3))),
    #                 ('agua', float(str(int(tanques[7][2])) + '.' + str(int(tanques[7][3])).zfill(3))),
    #                 ('temp', float(str(int(tanques[7][4])) + '.' + str(int(tanques[7][5])).zfill(3)))])),
    #])

    Tqs = dict([
        (tankid_global, dict([('nivel', float(str(int(tanques[0][0])) + '.' + str(int(tanques[0][1])).zfill(3))),
                     ('agua', float(str(int(tanques[0][2])) + '.' + str(int(tanques[0][3])).zfill(3))),
                     ('temp', float(str(int(tanques[0][4])) + '.' + str(int(tanques[0][5])).zfill(3)))])),
    ])

    print(f'Valores tanques: {Tqs}')

    try:
        connstr = "host=%s port=%s user=%s password=%s dbname=%s" % (PSQL_HOST, PSQL_PORT, PSQL_USER, PSQL_PASS, PSQL_DB)
        conn = psycopg2.connect(connstr)

        cur = conn.cursor()

        # tiempo_generar_cv = True

        # Ejecuta la consulta
        sqlquery = "SELECT num_tanque, producto, descripcion, capacidad FROM tanques;"
        cur.execute(sqlquery)

        # Obtener los resultados como objeto python
        rows = cur.fetchall()

        print(f'Rows: {rows}')

        # Cerrar la conexion con la base da datos
        cur.close()

        for row in rows:
            # Recupera el identificador del tanque
            tank_id = row[0]
            print(f'Tank_id: {tank_id}')

            if str(tank_id).zfill(2) != '01':
                continue

            num_tanque = 't' + str(tank_id)
            tank_key = str(row[0]).zfill(2)
            
            print(f'num_tanque : {num_tanque}')
            print(f'Tank_key : {tank_key}')

            # Recupera los 2 niveles correspondiente de la tabla, para poder interpolar
            cur = conn.cursor()
            sqlquery = "(select * from %s where nivel >= %s order by nivel limit 1) union all (select * from %s where nivel < %s order by nivel desc limit 1)" % (num_tanque, Tqs[tank_key]['nivel'], num_tanque, Tqs[tank_key]['nivel'])
            cur.execute(sqlquery)
            tqs_row = cur.fetchall()
            

            print(f'tqs_row: {tqs_row}')

            if len(tqs_row) > 1:
                # x1, y1, x2, y2, x
                x1 = tqs_row[1][0]
                y1 = tqs_row[1][1]

                x2 = tqs_row[0][0]
                y2 = tqs_row[0][1]

                # Recuperamos el nivel reportado por comunicaciones
                x = Tqs[tank_key]['nivel']

            val_alt = Tqs[tank_key]['nivel']
            val_agua = Tqs[tank_key]['agua']
            val_temp = Tqs[tank_key]['temp']
            val_vag = 0.0
            val_vol = 0.0

            print(f'val_alt.. {val_alt}')
            print(f'val_agua ... {val_agua}')
            print(f'val_temp .. {val_temp}')


            # Vamos a ver de que se trata
            if len(tqs_row) > 1:
                print("Tanque %s, x1=%s, y1=%s x2=%s, y2=%s, x=%s " % (tank_key, x1, y1, x2, y2, x))
                # Interpola con el siguiente valor
                y = y1 + (((y2 - y1) * (x - x1))/(x2 - x1))
                print("Volumen Interpolado : %s" % (y))
                # El volumen interpolado es nuestra medicion de hoy
                val_vol = y

            # Los coeficientes de expansion considerados son:
            # Gasolina - 0.00123
            # Diesel - 0.00083
            if row[1] == 'DIESEL':
                coe = 0.00083
            else:
                coe = 0.00123

            # Realiza el calculo
            val_tc = val_vol + val_vol * (coe * (15.0 - val_temp))
            now = datetime.now()
            fecha = now.strftime("%Y%m%d%H%M")

            print(f'val_tc ... {val_tc}')

            query = "SELECT id from monitoreo_tanques ORDER BY ID DESC LIMIT 1"
            cur.execute(query)
            id_cons = cur.fetchone()
            if not id_cons:
                consecutivo = 0
            else:
                consecutivo = id_cons[0]

            query = f"""INSERT INTO monitoreo_tanques (vr_tanque, vr_fecha, vr_volumen, vr_vol_ct, vr_agua, vr_temp, id) VALUES('{tank_key}', '{fecha}', '{"{:.4f}".format(val_vol)}', '{"{:.4f}".format(val_tc)}', '{val_agua}', '{val_temp}', {consecutivo + 1})"""
            cur.execute(query)
            conn.commit()

            query = "DELETE FROM monitoreo_tanques WHERE id not in (SELECT id from monitoreo_tanques ORDER BY ID DESC Limit 10 )"
            cur.execute(query)
            conn.commit()

            cur.close()
            # Manda llamar al calculo de entregas
            procesa_entregas(tank_id=tank_key, volumen=val_vol, volumen_ct=val_tc, temperatura=val_temp)

    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerr))


def procesa_entregas(tank_id, volumen, volumen_ct, temperatura):
    print(f"### procesa_entregas: tank_id {tank_id}, vol {volumen}, vol_ct {volumen_ct}, temp {temperatura}")

    try:
        connstr = "host=%s port=%s user=%s password=%s dbname=%s" % (PSQL_HOST, PSQL_PORT, PSQL_USER, PSQL_PASS, PSQL_DB)
        conn = psycopg2.connect(connstr)
        # Abrir un cursor para realizar las operaciones a la base de datos
        cur = conn.cursor()
    
        sqlquery = "SELECT vr_tanque, vr_fecha, vr_volumen, vr_vol_ct, vr_agua, vr_temp FROM monitoreo_tanques WHERE vr_tanque = '%s' ORDER BY id DESC LIMIT 2" % (tank_id)
        # Ejecuta la consulta
        cur.execute(sqlquery)
        # Obtener los resultados como objeto python
        rows = cur.fetchall()

        if not rows:
            return
        
        vol_act = rows[0]
        vol_ant = rows[-1]
        
        print(f'volumen anterior: {vol_ant}')
        print(f'volumen actual: {vol_act}')

        #Se realiza la diferencia entre los dos ultimos registros para detectar un aumento
        vol_dif = float(vol_act[2]) - float(vol_ant[2])
        print(f'Diferencia para ver si es descarga: {vol_dif}')

        #Recuperar valor base gsm_tanques con el vr tank inicia_enterga
        query = f"""SELECT inicia_entrega FROM tanques WHERE vr_tanque = \'{vol_act[0]}\'"""
        cur.execute(query)

        ini_entrega = cur.fetchone()
        print(f'Ini_entrega{ini_entrega[0]}')

        if vol_dif > 50 and ini_entrega[0] == 'False':
            print('### Empieza a registrar una entega ###')
            vol_ref = vol_ant[2]
            now = datetime.now()
            fecha = now.strftime("%Y%m%d%H%M")
            # Valores que vamos a usar de referencia para compararlos cuando finalize la descarga
            query = f"""UPDATE tanques set inicia_entrega = 'True', vol_ref = \'{vol_ref}\', fecha_ref = \'{fecha}\' WHERE vr_tanque = \'{vol_act[0]}\'"""
            cur.execute(query)
            conn.commit()
            print(f'Volumen referencia para entrega: {vol_ref}')


        query = f"""SELECT inicia_entrega FROM tanques WHERE vr_tanque = \'{vol_act[0]}\'"""
        cur.execute(query)
        ini_entrega = cur.fetchone()

        # Terminamos la validación
        if vol_dif <= 0 and ini_entrega[0] == 'True':
            fin_descarga = vol_ant[2]
            print(f'Fin descarga: {fin_descarga}')
            #Cambiamos la bandera de estado para que pueda registrar nuevamente un punto de referencia.
            query = "UPDATE tanques set inicia_entrega = 'False' WHERE vr_tanque = \'{0}\'".format(vol_act[0])
            cur.execute(query)
            conn.commit()
            #Se recuperan los datos almacenados en gsm_tanques partiendo del tanque_id
            query = "SELECT vol_ref, fecha_ref FROM tanques WHERE vr_tanque = \'{0}\'".format(vol_act[0])
            cur.execute(query)
            val_refe = cur.fetchone()

            now = datetime.now()
            fecha_fin = now.strftime("%Y%m%d%H%M")

            vol_resul = float(fin_descarga) - float(val_refe[0])
            print("####################################### Datos a insertar en Entregas #######################################")
            print(f'vr_tanque: {vol_act[0]}')
            print(f'fecha_ini: {val_refe[1]}')
            print(f'fecha_fin: {fecha_fin}')
            print(f'vr_volumen: {vol_resul}')
            print(f'vr_agua : {vol_act[4]}')
            print(f'vr_temp : {vol_act[5]}')
            
            query = "SELECT id FROM entregas ORDER BY ID DESC LIMIT 1"
            cur.execute(query)
            id_entrega = cur.fetchone
            
            if not id_entrega:
                id_cons = 0 
                
            else:
                id_cons = id_entrega[0]
            
            # Inserta la Entrega 
            query = f"""INSERT INTO Entregas (vr_tanque, fecha_ini, fecha_fin, vr_volumen, vr_agua, vr_temp, id, x) VALUES ('{vol_act[0]}', '{val_refe[1]}', '{fecha_fin}', '{vol_resul}', '{vol_act[4]}', '{vol_act[5]}', {id_cons},'1')"""
            cur.execute(query)
            conn.commit()

    except IOError as error:
        print("I/O error({0}): {1}".format(error.errno, error.strerr))
        return


def run_sync_client(client, modbus_calls=None):
    """Run sync client."""
    print("### Client starting")
    client.connect()
    if modbus_calls:
        modbus_calls(client)
    client.close()

def ProcesaInventario():

    team_client = setup_sync_client()
    print("### Inicia el procesamiento de Inventarios...")
    
    
    # Hace la consulta para solicitar la informacion tanque por tanque
    run_sync_client(team_client, modbus_calls=read_input_registers_call)
    


def setup_sync_client():
    """Run client setup."""
    print("### Create client object")

    client = ModbusSerialClient(
        port='/dev/ttyUSB0',  # serial port
        # Common optional paramers:
        framer=ModbusRtuFramer,
        timeout=120,
        retries=0,
            retry_on_empty=True,
        #    close_comm_on_error=False,.
        strict=True,
        # Serial setup parameters
        baudrate=19200,
        bytesize=8,
        parity="N",
        stopbits=1,
        #    handle_local_echo=False,
    )
    return client
    
def read_input_registers_call(client):
    """Demonstrate basic read/write calls."""
#    _handle_holding_registers(client)
#    _handle_coils(client)
#    _handle_discrete_input(client)
    _handle_input_registers(client)
    
    # _handle_input_registers_sensor(client)
    
    print("### End of Program")

if __name__ == "__main__":
    FSM_Core()

        
        