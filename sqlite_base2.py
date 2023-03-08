""" Реализрвано создание второй таблицы в БД
    Функции создания/октрытия и закрытия файла базы данных интегрированы в функции:
    - сохранить нового пользователя
    - вывод в Терминал данных всех пользователей 
    - вывод в Терминал пользователей, которыe родились ранее ... (дата)
    - удаление из базы данных пользователей, которые родились позже ... (дата))
    - изменение данных пользователя в базе данных 
    - получение данных пользователя по его id
    Реализована функция выдачи информации при ошибке в подкючении к базе данных. 
"""
import sqlite3
from sqlite3 import Error
from datetime import datetime

db_file_name = 'data.db'
db_table_name = 'users_data'
db_table_name_columns = 'user_id INTEGER, user_name TEXT, birth_date TEXT, register_date TEXT, modify_date TEXT'


# Convert curent DATE/TIME to TEXT
def get_date():
    return str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


# Convert REAL to DATE
def get_date_by_real(real):
    return datetime.timestamp(real)


# Convert TEXT to DATE
def get_date_by_text(date_time_str):
    try:
        datetime_object = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
    except ValueError as ve:
        print('ValueError :', ve)
        datetime_object = False
    return datetime_object


# connect to sql file
def connect_sql():
    global db_file_name
    try:
        # открыть файл базы данных. Если файла нет, он будет создан.
        con = sqlite3.connect(db_file_name)
        # print('SQL connected.')
        return con
    except Error:
        print(Error)


# Создаём таблицу, если её нет. Прописываем название столбцов таблицы с указанием их типов данных
def create_sql_table(con):
    global db_table_name, db_table_name_columns
    # создаём курсор, с его помощью производяться все действия с базой данных
    cursorObj = con.cursor()
    cursorObj.execute(
        f"CREATE TABLE IF NOT EXISTS {db_table_name} ( {db_table_name_columns} )")
    # вносим изменения в базу данных
    con.commit()
    # print('TABLE created/opened.')
    return cursorObj


# Добавление новых записей в таблицу (c проверкой, что данные в таблице отсутствуют)
def save_new_user(user_id, user_name, birth_date):
    db = connect_sql()
    cur = create_sql_table(db)
    # сделать выборку, которая удовлетворяет фильтру
    cur.execute(f"SELECT {user_id} FROM users_data WHERE user_id = {user_id}")
    if cur.fetchone() is None:                      # получить первый элемент из выборки
        now = get_date()
        cur.execute("INSERT INTO users_data VALUES (?, ?, ?, ?, ?)",
                    (user_id, user_name, birth_date, now, now))  # запись данных
        print(f'Пользователь с id {user_id} сохранён!')
        cur.execute("SELECT ROWID FROM users_data")
        print(
            f'В базе данных зарегистрировано { len(cur.fetchall())} пользователей.')
    else:
        print(f'Пользователь с id {user_id} ранее был зарегистрирован!')
    db.commit()  # вносим изменения в базу данных
    # закрыть файл базы данных.
    db.close()


# Вывод в Терминал элементов списка users
def view_all_user():
    db = connect_sql()
    cur = create_sql_table(db)
    # сделать выборку: ROWID - номер записи, * - все столбцы
    cur.execute("SELECT ROWID, * FROM users_data")
    # выбрать все записи из выборки
    print(cur.fetchall())
    # закрыть файл базы данных.
    db.close()


# Вывод в Терминал элементов списка users, возраст которых больше указанного
def view_older_user(date: int):
    db = connect_sql()
    cur = create_sql_table(db)
    cur.execute(
        f"SELECT ROWID, * FROM users_data WHERE birth_date > {date} ORDER BY birth_date DESC")
    # SELECT ROWID, * - сделать выборку по ROWID (номер записи) и * (все столбцы)
    # FROM users - из таблицы user
    # WHERE user_age >= age - используем условие выборки, где user_age >= age   (<, <>, =, >=, <=)
    # ORDER BY user_age DESC - упорядочить по user_age по возрастанию, DESC - по убыванию

    items = cur.fetchall()                          # выбрать все записи из выборки
    # print(cur.fetchmany(2))                       # выбрать только первые 2 записи из выборки
    # print(cur.fetchone()[1])                      # вывести из первой записи, 2ой элемент

    for item in items:
        print(f'Пользователь {item[2]} родился {item[3]}')
    # закрыть файл базы данных.
    db.close()


# Удаление элементов списка users, возраст которых меньше указанного
def delete_young_user(age: int):
    db = connect_sql()
    cur = create_sql_table(db)
    deleted_users = cur.execute(
        "DELETE FROM users_data WHERE birth_date > ?", (age,)).rowcount
    # rowcount - возвращает количество удалённых строк в БД
    db.commit()                                     # вносим изменения в базу данных
    print(f'Удалено {deleted_users} пользователей.')
    cur.execute("SELECT ROWID FROM users_data")
    print(
        f'В базе данных зарегистрировано { len(cur.fetchall())} пользователей.')
    # закрыть файл базы данных.
    db.close()


# Изменение данных пользователя
def change_user_data(user_id, user_name, birth_date):
    db = connect_sql()
    cur = create_sql_table(db)
    # cur.execute("UPDATE users SET user_age = ? WHERE user_id = ?", (user_age, user_id))     # Do this instead
    # cur.execute(f"UPDATE users SET user_age = {user_age} WHERE user_id = {user_id}")      # Never do this -- insecure!
    cur.execute("UPDATE users_data SET user_name=?, birth_date=?, modify_date=? WHERE user_id=? ",
                (user_name,  birth_date, get_date(), user_id))
    db.commit()                                     # вносим изменения в базу данных
    print(
        f'Данные пользователя с id {user_id} были изменены: {get_user_by_id(user_id)}')
    get_user_by_id(user_id)
    # закрыть файл базы данных.
    db.close()


# Получение данных пользователя по его id
def get_user_by_id(user_id):
    db = connect_sql()
    cur = create_sql_table(db)
    # cur.execute("UPDATE users SET user_age = ? WHERE user_id = ?", (user_age, user_id))     # Do this instead
    # cur.execute(f"UPDATE users SET user_age = {user_age} WHERE user_id = {user_id}")      # Never do this -- insecure!
    cur.execute(
        "SELECT ROWID,* FROM users_data WHERE user_id = ? ", (user_id,))
    user_data = cur.fetchone()
    # закрыть файл базы данных.
    db.close()
    return user_data


save_new_user(12345, 'Roman', '1979-03-18')
save_new_user(12346, 'Hanna', '1981-03-25')
# попытка записать существующего пользователя
save_new_user(12346, 'Hanna', '1981-03-25')
save_new_user(12347, 'Sofia', '2012-05-20')
view_all_user()
# показать пользователей, возраст которых больше или равно 20 лет
view_older_user('2003-03-08')
# удалить пользователей, возраст которых меньше 18 лет
delete_young_user('2005-03-08')
view_all_user()
# изменение данных пользователя
change_user_data(12346, 'Hanna', '1979-03-25')
view_all_user()
