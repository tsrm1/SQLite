import sqlite3 as sq

with sq.connect('sqlite.db') as con:          # контекст меньшера with автоматически закрывает базу данных без ошибок
    cur = con.cursor()

    # удаление таблицы users, если она существует
    cur.execute("DROP TABLE IF EXISTS users")
    # удаление таблицы games, если она существует
    cur.execute("DROP TABLE IF EXISTS games")

    # Создание таблицы users, если она не существует
    cur.execute("""CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    sex INTEGER NOT NULL DEFAULT 1,
    age INTEGER,
    score INTEGER    
    )""")

    # IF EXISTS - если существует, используется при удалении таблицы
    # IF NOT EXISTS - если не существует, используется при создании таблицы
    # PRIMARY KEY - значение должно быть уникальным
    # AUTOINCREMENT - значение автоматически увеличивается на 1
    # NOT NULL - значение не пустое (по умолчание пустая строка)
    # DEFAULT 1 - значение по умолчанию равно 1

    # Создание таблицы games, если она не существует
    cur.execute("""CREATE TABLE IF NOT EXISTS games (
    user_id INTEGER,
    score INTEGER,
    coin INTEGER
    )""")

    # Добавление записи в таблицу users, вариант 1: если указываем все поля
    cur.execute("INSERT INTO users VALUES (1, 'Bohdan', 1, 40, 3800)")  # нужна проверка на уникальность user_id !!!


    # Добавление записи в таблицу users, вариант 2: если указываем только необходимые поля
    cur.execute("INSERT INTO users (name, age, score) VALUES ('Roman', 34, 3000)")  # sex = 1 - по умолчанию
    cur.execute("INSERT INTO users (name, sex, age, score) VALUES ('Hanna', 2, 24, 4000)")
    cur.execute("INSERT INTO users (name, sex, age, score) VALUES ('Valeria', 2, 12, 2000)")
    cur.execute("INSERT INTO users (name, sex, age, score) VALUES ('Sofia', 2, 12, 3200)")

    # Добавление записи в таблицу games, вариант 3
    cur.execute("INSERT INTO games VALUES (?, ?, ?)", (1, 200, 3000))
    cur.execute("INSERT INTO games VALUES (?, ?, ?)", (2, 300, 3300))
    cur.execute("INSERT INTO games VALUES (?, ?, ?)", (3, 100, 3030))
    cur.execute("INSERT INTO games VALUES (?, ?, ?)", (4, 440, 3003))
    cur.execute("INSERT INTO games VALUES (?, ?, ?)", (5, 370, 3400))

    # Добавление записи в таблицу games, вариант 4 - добавление группы записей по одной
    games = [(3, 120, 3040),
             (2, 470, 3004),
             (4, 460, 3700),
             (1, 500, 3050)
             ]
    for game in games:
        cur.execute("INSERT INTO games VALUES (?, ?, ?)", game)

    # Добавление записи в таблицу games, вариант 5 - добавление группы записей сразу
    games = [(1, 120, 2040),
             (2, 470, 2004),
             (3, 460, 2700),
             (4, 500, 2050)
             ]
    cur.executemany("INSERT INTO games VALUES (?, ?, ?)", games)


    # делаем выборку всех элементов для таблицы users и отдельно для games
    cur.execute("SELECT ROWID, * FROM users")
    # выводим все записи в Terminal
    print('Выводим все записи таблицы users')
    for item in cur:
        print(item)
    print()

    # делаем выборку всех элементов для таблицы games
    cur.execute("SELECT ROWID, * FROM games")
    # выводим все записи в Terminal
    print('Выводим все записи таблицы games')
    for item in cur:
        print(item)
    print()

    # Выводим сводную таблицу из таблицы games, заменяя user_id именем ползователя
    cur.execute("""SELECT name, sex, games.score FROM games 
    LEFT JOIN users ON games.user_id = users.ROWID
    """)
    print('Выводим сводную таблицу из таблицы games, заменяя user_id именем ползователя')
    for item in cur:
        print(item)
    print()

    # Выводим сводную таблицу "Рейтинг", сортируя по score
    cur.execute("""SELECT name, sex, age, games.score as score FROM games
    JOIN users ON games.user_id = users.ROWID
    ORDER BY score DESC
    """)
    print('Выводим сводную таблицу "Рейтинг", сортируя по score')
    for item in cur:
        print(item)
    print()

    # Выводим сводную таблицу "Рейтинг", суммируя score и группируя по user_id
    cur.execute("""SELECT name, sex, age, sum(games.score) as score FROM games
    JOIN users ON games.user_id = users.ROWID
    GROUP BY name
    ORDER BY score DESC
    """)
    print('Выводим сводную таблицу "Рейтинг", суммируя score и группируя по name')
    for item in cur:
        print(item)
    print()


    # Выводим сводную таблицу, объединив две таблицы по user_id и score/coin. Можем указать источник данных
    cur.execute("""SELECT user_id, score, 'table 1' as tbl FROM users
    UNION SELECT user_id, coin, 'table 2' FROM games
    ORDER BY score DESC 
    """)
    print("""Выводим сводную таблицу, объединив две таблицы по user_id и score/coin. 
Можем указать источник данных, а также отсортировать по убыванию.""")
    for item in cur:
        print(item)
    print()

    # Выводим сводную таблицу, объединив две таблицы по user_id. Получаем только уникальные записи
    cur.execute("""SELECT user_id FROM users
    UNION SELECT user_id FROM games
    """)
    print('Выводим сводную таблицу, объединив две таблицы по user_id')
    for item in cur:
        print(item)
    print()

    # делаем выборку из БД по всем столбацам, котороые удовлетовряют условию. Сортируем по age
    cur.execute("SELECT * FROM users WHERE score BETWEEN 1000 AND 3000 ORDER BY age DESC LIMIT 5 OFFSET 1")
    # ORDER BY age DESC - упорядочить по age по возрастанию, DESC - по убыванию
    # LIMIT 2, 5 - выдать первые 5 записей, со смещением 2 записи (т.е. первые 2 записи будут удалены)
    # OFFSET 2 - смещение на 2 записи (т.е. первые 2 записи будут удалены)
    print('Выводим записи из таблицы user, котороые удовлетовряют условию BETWEEN. Сортируем по age')
    for item in cur:
        print(item)
    print()

    # Выводим записи из таблицы user, котороые удовлетовряют условию name LIKE '%an%'
    cur.execute("SELECT name FROM users WHERE name LIKE '%an%'")
    print('Выводим записи из таблицы user, котороые удовлетовряют условию LIKE %an%')
    for item in cur:
        print(list(item)[0])
    print()


    # Считаем количество игр, в которые играл игрок с именем удовлетовряющий условию name LIKE '%an%'
    cur.execute("SELECT count(name) as count FROM users WHERE name LIKE '%an%'")
    # count(name)
    # count(name) as count
    print('Cчитаем количество записей в таблице users, удовлетовряющих условию LIKE %an%')
    # вытаскиваем результат из типа CURSOR, вариант 1
    temp = tuple(cur)[0][0]
    print(temp)
    print()

    # Делаем выборку и считаем количество уникальных имён в таблице users
    cur.execute("SELECT count(DISTINCT name) as count FROM users")
    # cur.execute("SELECT DISTINCT name as count FROM users") # возвращает все уникальные имена
    # DISTINCT - только уникальные значения
    print('Cчитаем количество уникальных имён в таблице users')
    # вытаскиваем результат из типа CURSOR, вариант 2
    for item in cur:
        print(list(item)[0])
    print()


    # Вложенный запрос. Найти по имени значение coin в табл.1. Вывести из табл.2 все записи, у котор. score > coin и sex = 2
    cur.execute("SELECT score FROM users WHERE name LIKE 'Sof%'")
    print('Вложенный запрос. Запрос №1: ', cur.fetchall())
    cur.execute("""SELECT name, sex, games.coin FROM games 
    LEFT JOIN users ON games.user_id = users.ROWID WHERE coin > 3200 AND sex = 2""")
    print('Вложенный запрос. Запрос №2: ', cur.fetchall())

    cur.execute("""SELECT name, sex, games.coin FROM games 
    LEFT JOIN users ON games.user_id = users.ROWID WHERE coin > (SELECT score FROM users WHERE name LIKE 'Sof%') AND sex = 2""")
    print('Вложенный запрос. Запрос №1+№2: ', cur.fetchall())
    print()

    # при использовании: with sq.connect('data.sqlite') as con:
    # операторы ниже не нужны
    # con.commit()
    # con.close()