import sqlite3 as sq

def load_avatar(n):
    try:
        with open(f'sqlite_avatar/{n}.png', 'rb') as f:
            return f.read()
    except IOError as e:
        print(e)
        return False

def save_avatar(f_name, data):
    try:
        with open(f'sqlite_avatar/out/{f_name}.png', 'wb') as f:
            return f.write(data)
    except IOError as e:
        print(e)
        return False


users = [[1234567890, 'Patricia Patrick', 13, 500],
         [2345678901, 'Leslie Rhodes', 15, 600],
         [3456789012, 'June Zimmerman', 17, 700],
         [4567890123, 'David Gibson', 14, 400],
         [5678901234, 'Harvey Young', 16, 500],
         [6789012345, 'Jose Lane', 13, 800],
         [7890123456, 'William Williams', 16, 300],
         [8901234567, 'Donald Lopez', 14, 400],
         ]

with sq.connect('sqlite_avatar.db') as con:
    con.row_factory = sq.Row    # перевод доступа к записям БД как к типу словаря, type dict
    cur = con.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS users (
                        user_id INTEGER,
                        name TEXT,
                        age INTEGER,
                        score INTEGER,
                        avatar BLOB)
    """)

    for n in range(8):
        img = load_avatar(n+1)
        if img:
            binary = sq.Binary(img)
            cur.execute("INSERT INTO users VALUES (?, ?, ?, ?, ?)", (users[n][0], users[n][1], users[n][2], users[n][3], binary))
    print('Загрузка аватарок в БД завершена.')

    cur.execute("SELECT avatar FROM users")
    items = cur.fetchall()
    n = 0
    for item in items:
        n +=1
        save_avatar(n, item['avatar'])


    #     img = load_avatar(n+1)
    #     if img:
    #         binary = sq.Binary(img)
    #         cur.execute("INSERT INTO users VALUES (?, ?, ?, ?, ?)", (users[n][0], users[n][1], users[n][2], users[n][3], binary))
    print('Запись аватарок в отдельные файлы завершена.')

