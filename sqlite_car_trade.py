import sqlite3 as sq

with sq.connect('sqlite_car_trade.db') as con:
    con.row_factory = sq.Row    # перевод доступа к записям БД как к типу словаря, type dict
    cur = con.cursor()

    cars = [('Audi', 50000, '2023-01-01'),
            ('Mercedes', 55000, '2023-01-01'),
            ('Skoda', 9000, '2022-06-01'),
            ('Volvo', 30000, '2023-01-01'),
            ('Bentley', 350000, '2023-01-01')
            ]


    cur.executescript("""CREATE TABLE IF NOT EXISTS cars (
                        car_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        model TEXT,
                        price INTEGER,
                        sale_start TEXT,
                        sold TEXT DEFAULT '',
                        buyer_id INTEGER DEFAULT 0
                        );
                        
                        CREATE TABLE IF NOT EXISTS clients (
                        buyer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT,
                        trade_in INTEGER,
                        buy INTEGER);
    
    """)
    cur.executemany("INSERT INTO cars VALUES (NULL, ?, ?, ?, NULL, NULL)", cars)

    # просмотр содержимого 2х таблиц
    cur.execute("SELECT * FROM cars")
    print('В наличии есть следующие машины "cars"')
    for item in cur:
        # print(item)
        print(item['car_id'], item['model'], item['price'], item['sale_start'], item['sold'], item['buyer_id'])
        # print(dict(item))

    print()
    cur.execute("SELECT * FROM clients")
    print('Зарегистрированные покупатели "clients"')
    for item in cur:
        # print(item)
        print(item['buyer_id'], item['name'], item['trade_in'], item['buy'])
        # print(dict(item))
    print()

    buy_car_id = int(input("Введите id машины, которую хотите купить? "))
    cur.execute("SELECT * FROM cars WHERE ROWID = ?", (buy_car_id,))
    sale_date = list(cur.fetchone())[4]

    if sale_date == None:
        buyer_full_name = 'Краснов Игрь Петрович'
        buyer_old_car = 'Запорожец'
        buyer_trade_in = 1000
        buyer_sale_date = '2023-04-01'

        cur.execute("INSERT INTO cars VALUES (NULL, ?, ?, ?, NULL, NULL)", (buyer_old_car, buyer_trade_in, buyer_sale_date))
        last_row_id = cur.lastrowid
        cur.execute("INSERT INTO clients VALUES (NULL, ?, ?, ?)",
                    (buyer_full_name, last_row_id, buy_car_id))
        last_row_id = cur.lastrowid
        cur.execute("UPDATE cars SET sold=?, buyer_id=? WHERE ROWID = ?", (buyer_sale_date, last_row_id, buy_car_id))
        # print(last_row_id)
    else:
        print("Сожалею, данная машина уже продана!")

    # просмотр содержимого 2х таблиц
    cur.execute("SELECT * FROM cars")
    print('Выводим таблицу "cars"')
    for item in cur:
        # print(item)
        print(item['car_id'], item['model'], item['price'], item['sale_start'], item['sold'], item['buyer_id'])
        # print(dict(item))
    print()
    cur.execute("SELECT * FROM clients")
    print('Выводим таблицу "clients"')
    for item in cur:
        # print(item)
        print(item['buyer_id'], item['name'], item['trade_in'], item['buy'])
        # print(dict(item))
    print()
