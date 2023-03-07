""" Создание/октрытие и закрытие файла базы данных.
    Создание табицы в файле базы данных.
    Добавление, изменение, удаление записей.
    Выборка данных используя условие.
"""
import sqlite3


db = sqlite3.connect('data.db')                     # открыть файл базы данных. Если файла нет, он будет создан.
print('Файл базы данных подключен.')

cur = db.cursor()                                   # создать курсор, с его помощью производяться все действия с базой данных


# Создаём таблицу, если её нет. Прописываем название столбцов таблицы с указанием их типов данных
cur.execute("""CREATE TABLE IF NOT EXISTS users (      
            user_id TEXT,           
            user_name TEXT,        
            user_age INT,         
            user_password TEXT     
            )""")
db.commit()                                         # вносим изменения в базу данных
print('Таблица с заголовками базы данных создана.')


# Добавление новых записей в таблицу (c проверкой, что данные в таблице отсутствуют)
def save_new_user(user_id, user_name, user_age, user_password):
    global db
    global cur
    cur.execute(f"SELECT {user_id} FROM users WHERE user_id = {user_id}")   # сделать выборку, которая удовлетворяет фильтру
    if cur.fetchone() is None:                      # получить первый элемент из выборки
        cur.execute("INSERT INTO users VALUES (?, ?, ?, ?)", (user_id, user_name, user_age, user_password)) # запись данных
        print(f'Пользователь с id {user_id} сохранён!')
    else:
        print(f'Пользователь с id {user_id} ранее был зарегистрирован!')
    db.commit()  # вносим изменения в базу данных


# Вывод в Терминал элементов списка users
def view_all_user():
    global db
    global cur
    cur.execute("SELECT ROWID, * FROM users")       # сделать выборку: ROWID - номер записи, * - все столбцы
    print(cur.fetchall())                           # выбрать все записи из выборки
    # print(cur.fetchmany(2))                       # выбрать только первые 2 записи из выборки
    # print(cur.fetchone()[1])                      # вывести из первой записи, 2ой элемент

# Вывод в Терминал элементов списка users, возраст которых больше указанного
def view_older_user(age: int):
    global db
    global cur
    cur.execute(f"SELECT ROWID, * FROM users WHERE user_age >= {age} ORDER BY user_age DESC")
    # SELECT ROWID, * - сделать выборку по ROWID (номер записи) и * (все столбцы)
    # FROM users - из таблицы user
    # WHERE user_age >= age - используем условие выборки, где user_age >= age   (<, <>, =, >=, <=)
    # ORDER BY user_age DESC - упорядочить по user_age по возрастанию, DESC - по убыванию

    items = cur.fetchall()                          # выбрать все записи из выборки
    # print(cur.fetchmany(2))                       # выбрать только первые 2 записи из выборки
    # print(cur.fetchone()[1])                      # вывести из первой записи, 2ой элемент

    for item in items:
        print('Пользователю', item[2], item[3], 'лет.')

# Удаление элементов списка users, возраст которых меньше указанного
def delete_young_user(age: int):
    global db
    global cur
    cur.execute("DELETE FROM users WHERE user_age < ?", (age,))
    db.commit()                                     # вносим изменения в базу данных

# Изменение данных пользователя
def change_user_data(user_id, user_name, user_age: int, user_password):
    global db
    global cur
    # cur.execute("UPDATE users SET user_age = ? WHERE user_id = ?", (user_age, user_id))     # Do this instead
    # cur.execute(f"UPDATE users SET user_age = {user_age} WHERE user_id = {user_id}")      # Never do this -- insecure!


    cur.execute("UPDATE users SET user_name=?, user_age=?, user_password=? WHERE user_id=? ", (user_name, user_age, user_password, user_id))

    db.commit()                                     # вносим изменения в базу данных

save_new_user('12345', 'Roman', 43, 'qwerty')
save_new_user('12346', 'Hanna', 40, 'qwerty')
save_new_user('12346', 'Hanna', 40, 'qwerty')       # попытка записать существующего пользователя
save_new_user('12347', 'Sofia', 10, 'qwerty')
view_all_user()
view_older_user(20)                                 # показать пользователей, возраст которых больше или равно 20 лет
delete_young_user(18)                               # удалить пользователей, возраст которых меньше 18 лет
view_all_user()
change_user_data('12346', 'Hanna', 43, 'Montanna')  # изменение данных пользователя
view_all_user()
db.close()                                          # закрыть файл базы данных.