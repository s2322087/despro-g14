import os
import datetime
import urllib.request
import sqlite3
from bs4 import BeautifulSoup

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
    start_date = datetime.date(2023, 4, 7)
    end_date = datetime.date(2023, 12, 20)
    interval = datetime.timedelta(days=7)  

    # SQLiteデータベースに接続
    conn = sqlite3.connect('weather_data2.db')
    cursor = conn.cursor()

    # テーブルが存在しない場合は作成
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT,
                        time TEXT,
                        pressure_local REAL,
                        pressure_sea REAL,
                        precipitation REAL,
                        temperature REAL,
                        dew_point REAL,
                        vapor_pressure REAL,
                        humidity REAL,
                        wind_speed REAL,
                        wind_direction REAL,
                        sunshine_duration REAL,
                        global_radiation REAL,
                        snowfall REAL,
                        snow_depth REAL
                    )''')

    date = start_date
    while date <= end_date:
        url = "http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_s1.php?" \
              "prec_no=44&block_no=47662&year=%d&month=%d&day=%d&view=" % (date.year, date.month, date.day)

        data_per_day = scraping(url, date)

        for dpd in data_per_day:
            # データをデータベースに挿入
            cursor.execute('''INSERT INTO weather (date, time, pressure_local, pressure_sea,
                                                    precipitation, temperature, dew_point,
                                                    vapor_pressure, humidity, wind_speed,
                                                    wind_direction, sunshine_duration,
                                                    global_radiation, snowfall, snow_depth)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', dpd[:15])  # 最初の15個のデータを使う

        date += interval  

    # 変更を保存
    conn.commit()

    # 接続を閉じる
    conn.close()

    print("done")

if __name__ == '__main__':
    create_output()