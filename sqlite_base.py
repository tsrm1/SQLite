""" 
    Функции создания/октрытия и закрытия файла базы данных интегрированы в функции:
    - сохранить нового пользователя
    - вывод в Терминал данных всех пользователей 
    - вывод в Терминал пользователей, которым больше ... лет
    - удаление из базы данных пользователей, которым меньше ... лет
    - изменение данных пользователя в базе данных 
    - получение данных пользователя по его id
    Реализована функция выдачи информации при ошибке в подкючении к базе данных. 
"""
import sqlite3
from sqlite3 import Error

db_file_name = 'data.db'
db_table_name = 'users'
db_table_name_columns = 'user_id TEXT, user_name TEXT, user_age INT, user_password TEXT'


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
def save_new_user(user_id, user_name, user_age, user_password):
    db = connect_sql()
    cur = create_sql_table(db)
    # сделать выборку, которая удовлетворяет фильтру
    cur.execute(f"SELECT {user_id} FROM users WHERE user_id = {user_id}")
    if cur.fetchone() is None:                      # получить первый элемент из выборки
        cur.execute("INSERT INTO users VALUES (?, ?, ?, ?)", (user_id,
                    user_name, user_age, user_password))  # запись данных
        print(f'Пользователь с id {user_id} сохранён!')
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
    cur.execute("SELECT ROWID, * FROM users")
    # выбрать все записи из выборки
    print(cur.fetchall())
    # закрыть файл базы данных.
    db.close()


# Вывод в Терминал элементов списка users, возраст которых больше указанного
def view_older_user(age: int):
    db = connect_sql()
    cur = create_sql_table(db)
    cur.execute(
        f"SELECT ROWID, * FROM users WHERE user_age >= {age} ORDER BY user_age DESC")
    # SELECT ROWID, * - сделать выборку по ROWID (номер записи) и * (все столбцы)
    # FROM users - из таблицы user
    # WHERE user_age >= age - используем условие выборки, где user_age >= age   (<, <>, =, >=, <=)
    # ORDER BY user_age DESC - упорядочить по user_age по возрастанию, DESC - по убыванию

    items = cur.fetchall()                          # выбрать все записи из выборки
    # print(cur.fetchmany(2))                       # выбрать только первые 2 записи из выборки
    # print(cur.fetchone()[1])                      # вывести из первой записи, 2ой элемент

    for item in items:
        print(f'Пользователю {item[2]} сейчас {item[3]} лет.')
    # закрыть файл базы данных.
    db.close()


# Удаление элементов списка users, возраст которых меньше указанного
def delete_young_user(age: int):
    db = connect_sql()
    cur = create_sql_table(db)
    cur.execute("DELETE FROM users WHERE user_age < ?", (age,))
    db.commit()                                     # вносим изменения в базу данных
    print(f'Удаление пользователей, младше {age} лет выполнено.')
    # закрыть файл базы данных.
    db.close()


# Изменение данных пользователя
def change_user_data(user_id, user_name, user_age: int, user_password):
    db = connect_sql()
    cur = create_sql_table(db)
    # cur.execute("UPDATE users SET user_age = ? WHERE user_id = ?", (user_age, user_id))     # Do this instead
    # cur.execute(f"UPDATE users SET user_age = {user_age} WHERE user_id = {user_id}")      # Never do this -- insecure!
    cur.execute("UPDATE users SET user_name=?, user_age=?, user_password=? WHERE user_id=? ",
                (user_name, user_age, user_password, user_id))
    db.commit()                                     # вносим изменения в базу данных
    print(
        f'Данные пользователя с id {user_id} были изменены: {get_user_by_id(user_id)}')
    get_user_by_id(user_id)
    # закрыть файл базы данных.
    db.close()


# Изменение данных пользователя
def get_user_by_id(user_id):
    db = connect_sql()
    cur = create_sql_table(db)
    # cur.execute("UPDATE users SET user_age = ? WHERE user_id = ?", (user_age, user_id))     # Do this instead
    # cur.execute(f"UPDATE users SET user_age = {user_age} WHERE user_id = {user_id}")      # Never do this -- insecure!
    cur.execute(
        "SELECT ROWID,* FROM users WHERE user_id = ? ", (user_id,))
    user_data = cur.fetchone()
    # закрыть файл базы данных.
    db.close()
    return user_data


save_new_user('12345', 'Roman', 43, 'qwerty')
save_new_user('12346', 'Hanna', 40, 'qwerty')
# попытка записать существующего пользователя
save_new_user('12346', 'Hanna', 40, 'qwerty')
save_new_user('12347', 'Sofia', 10, 'qwerty')
view_all_user()
# показать пользователей, возраст которых больше или равно 20 лет
view_older_user(20)
# удалить пользователей, возраст которых меньше 18 лет
delete_young_user(18)
view_all_user()
# изменение данных пользователя
change_user_data('12346', 'Hanna', 43, 'Montanna')
view_all_user()
