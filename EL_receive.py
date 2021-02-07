import socket
import time
import datetime
import csv
import os
import json
import math
from pathlib import Path

import yaml
from influxdb import InfluxDBClient

import ellp

ip = "0.0.0.0"
port = 3610
set_ch_telegram = b'\x10\x81\x00\x00\x05\xFF\x01\x02\x87\x01\x61\x01\xB6\x02\x01\x10'

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((ip, port))

csv_files_path = "F:/EL_CSV/log_data"

def notdir_find(dir_path):
    down_path = os.path.normpath('%s/../' %dir_path)
    dirname = os.path.basename(dir_path)
    return dirname, down_path

def alldir_mkdir(dir_path):
    dirname, down_path = notdir_find(dir_path)
    dir_list = ['%s' %dirname]
    if os.path.exists(down_path):
        try:
            os.mkdir(dir_path)
        except FileExistsError:
            None
    else:
        while not os.path.exists(down_path):
            try:
                dirname, down_path = notdir_find(down_path)
                dir_list.append(dirname)
                os.mkdir(down_path)
            except FileNotFoundError:
                dirname, down_path = notdir_find(down_path)
                dir_list.append(dirname)
                # os.mkdir(down_path)
                continue
            except FileExistsError:
                break
        dir_list.reverse()
        for file in dir_list:
            try:
                down_path = os.path.join(down_path, file)
                os.mkdir(down_path)
            except FileExistsError:
                print('ディレクトリはすでに作成されています。')
    return None

#先頭行のカラムの作成
column_list = ["time"]
for i in range(1, 17):
    column_list.append("ch" + str(i))

#与えられた時間に対して実行した際に新たに時間(終了時間)を定義し、計測時間、所要時間、その日の年月日を出力

def telegram_distributionboard(response, start_time, interval, count):
    end_time = time.time()
    if response[1][0] == "172.24.7.223":
        telegram = response[0].hex()
        total_time = end_time - start_time - interval*count
        print("通信所要時間：" + str(total_time))
        half_time = total_time / 2
        measurement_time = end_time - half_time - interval*count
        # measurement_time = math.floor(measurement_time)
        measurement_time = datetime.datetime.fromtimestamp(measurement_time)
        print("計測時刻：" + str(measurement_time))
        # print(telegram)
    elif response[1][0] == "172.24.7.226":
        telegram = response[0].hex()
        total_time = end_time - start_time - interval*count
        print("通信所要時間：" + str(total_time))
        half_time = total_time / 2
        measurement_time = end_time - half_time - interval*count
        # measurement_time = math.floor(measurement_time)
        measurement_time = datetime.datetime.fromtimestamp(measurement_time)
        print("計測時刻：" + str(measurement_time))
        # print(telegram)
    return telegram, measurement_time

def make_telegram_list(response, measurement_time):
    #レスポンス電文を4Bで区切って配列へ
    byte4_response = [response[i:i+8] for i in range(0, len(response), 8)]
    #ETD(計測データ)の部分のみ取得
    edt = byte4_response[4:]
    #EDTを10進数へ変換
    edt_10 = [int(i, 16)for i in edt]
    #データの0列目に時間を計測時間を挿入
    edt_10.insert(0, str(measurement_time))
    return edt_10


while True:
    try:
        #ファイル群一覧の取得
        interval = 0.05
        response = sock.recvfrom(2048)
        s_time = time.time()
        count = 1
        if response[1][0] == "127.0.0.1":
            start_time = float(response[0].decode())
            response = sock.recvfrom(2048)
            if response[1][0] == "172.24.7.223":
                cottage_number = str(101)
                print("-----------------%s-------------------" %(cottage_number))
                telegram, measurement_time = telegram_distributionboard(response, start_time, interval, count)
                today_date, today_time = str(measurement_time).split()
                file_folder_path = os.path.join(csv_files_path, cottage_number)
                alldir_mkdir(file_folder_path)
                csv_list = [file for file in os.listdir(file_folder_path)]
                today_csv = str(today_date) + ".csv"
                today_csv_path = os.path.join(file_folder_path, today_csv)
                if today_csv not in csv_list:
                    with open(today_csv_path, "w", newline="", encoding="UTF-8") as w:
                        writer = csv.writer(w)
                        writer.writerow(column_list)
                else:
                    file = open(today_csv_path, "a", newline="", encoding="UTF-8")
                    edt_10 = make_telegram_list(telegram, measurement_time)
                    print(edt_10)
                    writer = csv.writer(file)
                    writer.writerow(edt_10)
                    file.close()
            count += 1
            response = sock.recvfrom(2048)
            if response[1][0] == "172.24.7.226":
                # telegram_101 = response[0]
                # print(telegram_101)
                cottage_number = str(102)
                print("-----------------%s-------------------" %(cottage_number))
                telegram, measurement_time = telegram_distributionboard(response, start_time, interval, count)
                today_date, today_time = str(measurement_time).split()
                file_folder_path = os.path.join(csv_files_path, cottage_number)
                alldir_mkdir(file_folder_path)
                csv_list = [file for file in os.listdir(file_folder_path)]
                today_csv = str(today_date) + ".csv"
                today_csv_path = os.path.join(file_folder_path, today_csv)
                if today_csv not in csv_list:
                    with open(today_csv_path, "w", newline="", encoding="UTF-8") as w:
                        writer = csv.writer(w)
                        writer.writerow(column_list)
                else:
                    file = open(today_csv_path, "a", newline="", encoding="UTF-8")
                    edt_10 = make_telegram_list(telegram, measurement_time)
                    print(edt_10)
                    writer = csv.writer(file)
                    writer.writerow(edt_10)
                    file.close()
                    # InfluxDBに書き込む
                    conf_yml = Path(__file__).parent/'conf.yml'
                    with open(conf_yml, 'r') as f:
                        conf = yaml.safe_load(f)
                    try:
                        line_protocol = ellp.el_socket_lp(column_list, edt_10)
                        client = InfluxDBClient(**conf['InfluxDB'])
                        client.write_points(line_protocol)
                    except:
                        pass
            count += 1
        else:
            None
            continue
        # print(time.time() - s_time)
        print("===================================================")
    except UnicodeDecodeError as u:
        error_log = os.path.normpath("%s/../error_time.txt" %(__file__))
        with open(error_log, "a", newline="", encoding="UTF-8") as a:
            a.write(str(measurement_time) + " : " + str(u) + "\r\n")
        continue
    except PermissionError:
        pass
sock.close()
