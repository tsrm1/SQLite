import sqlite3 as sq
from datetime import datetime

db_file = 'sqlite_car_trade.db'

db_file_backup = db_file + '_BackUP' + '_' + datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.sql'

with sq.connect(db_file) as con:
    cur = con.cursor()

    # for sql in con.iterdump():
    #     print(sql)

    with open(db_file_backup, 'w') as f:    # сохраняем в файл-бэкап SQL-команды восстановления БД
        for sql in con.iterdump():
            f.write(sql)
        print('Создан файл dump: ', db_file_backup)

with sq.connect(db_file+'_recovered.db') as con:
    cur = con.cursor()

    with open(db_file_backup, 'r') as f:    # восстанавливаем БД с помощью файла SQL-команд
        sql = f.read()
        cur.executescript(sql)
        print('Файл БД восстановлен из бэкапа: ', db_file_backup)
