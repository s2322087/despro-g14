import os
import datetime
import urllib.request
import sqlite3
from bs4 import BeautifulSoup
import csv

def str2float(weather_data):
    try:
        return float(weather_data)
    except:
        return 0

def scraping(url, date):
    html = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(html, 'html.parser')
    trs = soup.find("table", {"class": "data2_s"})

    data_list_per_hour = []

    for tr in trs.findAll('tr')[10::24]:
        tds = tr.findAll('td')

        if len(tds) < 14:
            break

        data_list = [date, tds[0].string]
        data_list.extend([str2float(td.string) for td in tds[1:]])

        data_list_per_hour.append(data_list)

    return data_list_per_hour


def create_output():
    start_date = datetime.date(2023, 4, 1)
    end_date = datetime.date(2023, 12, 20)
    interval = datetime.timedelta(days=7)  

    # SQLiteデータベースに接続
    conn = sqlite3.connect('weather_data.db')
    cursor = conn.cursor()

    # テーブルが存在しない場合は作成
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT,
                        time TEXT,
                        pressure_local REAL,
                        pressure_local_sea REAL,
                        precipitation REAL,
                        temperature REAL,
                        wind_speed REAL,
                        sunshine_duration REAL
                    )''')

    date = start_date
    while date <= end_date:
        url = "http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_s1.php?" \
              "prec_no=44&block_no=47662&year=%d&month=%d&day=%d&view=" % (date.year, date.month, date.day)

        data_per_day = scraping(url, date)

        for dpd in data_per_day:
            # データをデータベースに挿入
            cursor.execute('''INSERT INTO weather (date, time, pressure_local,
                                                    pressure_local_sea, precipitation,
                                                    temperature, wind_speed,
                                                    sunshine_duration)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', dpd[:8])  # 最初の8個のデータを使う

        date += interval  

    # 変更を保存
    conn.commit()


    print("weather_data.dbを作成しました。")

if __name__ == '__main__':
    create_output()

    conn = sqlite3.connect('weather_data.db')
    cursor = conn.cursor()

    # CSVファイルをテーブルに挿入
    with open('local.csv', 'r') as file:
        csv_reader = csv.reader(file)
        
        # ヘッダー行をスキップ
        header = next(csv_reader)
        
        # テーブルが存在しない場合は作成
        cursor.execute('''CREATE TABLE IF NOT EXISTS local (
                           date TEXT,
                           "number of steps" INTEGER,
                           "Part Time Job" INTEGER
                         )''')

        for row in csv_reader:
            cursor.execute('INSERT INTO local VALUES (?, ?, ?)', tuple(row))

    # 変更を保存
    conn.commit()

    # 接続を閉じる
    conn.close()

    print("done")