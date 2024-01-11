import sqlite3
import csv

# データベースに接続
conn = sqlite3.connect('weather_data.db')
cursor = conn.cursor()

# CSVファイルをテーブルに挿入
with open('local.csv', 'r') as file:
    csv_reader = csv.reader(file)
    
    # ヘッダー行をスキップ
    header = next(csv_reader)
    
    # テーブルが存在しない場合は作成
    cursor.execute('''CREATE TABLE IF NOT EXISTS local (
                        date, number of steps, Part Time Job
                     )''')

    for row in csv_reader:
        cursor.execute('INSERT INTO local VALUES (?, ?, ?)', tuple(row))

# 変更を保存
conn.commit()

# 接続を閉じる
conn.close()

