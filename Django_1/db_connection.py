import datetime
# Бібліотека для SQLite (БД 1)
import sqlite3
# Бібліотека для PostgreSQL (БД 2)
import psycopg2
# Бібліотека для  MySQL (БД 3)
import mysql.connector


# Команда для створення та початкового заповнення таблиці обліку абонентів
# Варіант 25: Довідкова інформаційна система обліку телефонних номерів абонентів АТС;
# Виконюється один раз до початку роботи з адмінкою
def create_sqlite_table():
    # Створення підключення до бази даних SQLite (за відсутності файлу створюється новий)
    Lite_con = sqlite3.connect('C:\Temp\lab1.db')
    Lite_cur = Lite_con.cursor()
    # Створення таблиці
    Lite_cur.execute('''CREATE TABLE subscriptions
                    (id INTEGER PRIMARY KEY, telephone_number TEXT, tariff_type TEXT, paid_for INTEGER)''')

    # Заповнення таблиці
    # Команда executemany() повторює введену команду для кожного набору значень в entry_list
    entry_list = [
        (0, '+380 44 102-36-72', 'Unlimited', 1),
        (1, '+380 45 948-92-27', 'Entertainment', 1),
        (2, '+380 44 752-55-22', 'Unlimited', 1),
        (3, '+380 47 222-22-11', 'Budget', 0),
        (4, '+380 48 736-75-39', 'Entertainment', 1),
        (5, '+380 40 948-64-76', 'Entertainment', 1),
        (6, '+380 42 948-65-54', 'Budget', 1),
        (7, '+380 44 675-32-69', 'Unlimited', 0),
    ]
    Lite_cur.executemany("INSERT INTO subscriptions VALUES (?, ?, ?, ?)", entry_list)
    # Закінчення транзакції
    Lite_con.commit()
    # Закриття підключення
    Lite_con.close()


# Функція для створення БД2 postgreSQL
# Виконюється один раз до початку роботи з адмінкою
def create_postgres_database():
    # Створення підключення
    postgres_con = psycopg2.connect(user="postgres", password="DjangoLab", host="localhost")
    postgres_con.autocommit = True
    postgres_cur = postgres_con.cursor()
    # Створення БД
    postgres_cur.execute("CREATE DATABASE djangolab1;")
    # Закриття підключення
    postgres_con.close()


# Функція для створення БД3 MySQL
# Виконюється один раз до початку роботи з адмінкою
def create_mysql_database():
    # Створення підключення
    mysql_con = mysql.connector.connect(user="Python_app", password="DjangoLab", host="localhost")
    mysql_cur = mysql_con.cursor()
    # Створення БД
    mysql_cur.execute("CREATE DATABASE djangolab1;")
    # Закінчення транзакції
    mysql_con.commit()
    # Закриття підключення
    mysql_con.close()


# create_sqlite_table()
# create_postgres_database()
# create_mysql_database()


# Функція для створення нового введення в таблиці
def create(id_val, tel_num, tar_type, paid_for):
    # Створення підключення
    Lite_con = sqlite3.connect('C:\Temp\lab1.db')
    Lite_cur = Lite_con.cursor()
    # Команда вставлення
    # ? заміняється введеними значеннями
    Lite_cur.execute("INSERT or REPLACE INTO subscriptions VALUES (?, ?, ?, ?)", (id_val, tel_num, tar_type, paid_for))
    # Закінчення транзакції
    Lite_con.commit()
    # Закриття підключення
    Lite_con.close()


# Функція для зчитування значень таблиці
# Можна надати неповний телефониий номер, що поверне всі номери з введеною частиною номера
def read(tel_num=None, tar_type=None, paid_for=None):
    # Створення підключення
    Lite_con = sqlite3.connect('C:\Temp\lab1.db')
    Lite_cur = Lite_con.cursor()
    # Створення команди зчитування
    # Через специфіку мови SQL, потребує дерево умов для корректного форматування
    command = "SELECT * FROM subscriptions"
    if tel_num:
        command += " WHERE telephone_number LIKE '%" + tel_num + "%'"
    if tel_num and tar_type:
        command += " AND tariff_type = '" + tar_type + "'"
        if paid_for:
            command += " AND paid_for = " + paid_for
    elif tar_type:
        command += " WHERE tariff_type = '" + tar_type + "'"
        if paid_for:
            command += " AND paid_for = " + paid_for
    elif tel_num and paid_for:
        command += " AND paid_for = " + paid_for
    else:
        if paid_for:
            command += " WHERE paid_for = " + paid_for
    command += " ORDER BY id"

    # Виконання команди
    Lite_cur.execute(command)
    entry_list = Lite_cur.fetchall()
    # Закінчення транзакції
    Lite_con.commit()
    # Закриття підключення
    Lite_con.close()
    # Функція повертає вcі отримані значення
    return entry_list


# Функція змінює тариф та/або стан оплати для вказаного номеру телефону
def update(tel_num, tar_type=None, paid_for=None):
    # Створення підключення
    Lite_con = sqlite3.connect('C:\Temp\lab1.db')
    Lite_cur = Lite_con.cursor()
    # Якщо не обрано значення для зміни, повертає помилку
    if tar_type == '' and paid_for == '':
        raise ValueError("Не вказано змінну, що потребує змін")

    # Формулювання команди
    # Через специфіку мови SQL, потребує дерево умов для корректного форматування
    command = "UPDATE subscriptions SET "
    if tar_type:
        command += "tariff_type = '"+tar_type+"'"
    if tar_type and paid_for:
        command += ", "
    if paid_for:
        command += "paid_for = "+paid_for
    command += " WHERE telephone_number = '"+tel_num+"'"

    # Виконання команди
    Lite_cur.execute(command)
    # Закінчення транзакції
    Lite_con.commit()
    # Закриття підключення
    Lite_con.close()


# Функція для видалення введення в таблиці за певним телефонним номером
def delete(tel_num):
    # Створення підключення
    Lite_con = sqlite3.connect('C:\Temp\lab1.db')
    Lite_cur = Lite_con.cursor()
    # Виконання команди видалення
    Lite_cur.execute("DELETE FROM subscriptions WHERE telephone_number = ?", tel_num)
    # Закінчення транзакції
    Lite_con.commit()
    # Закриття підключення
    Lite_con.close()


# Функція експорту з БД1 до БД2, та з БД2 до БД3 з виконанням додаткової умови та SQL запиту
def export():
    # Створення підключення до БД SQLite
    Lite_con = sqlite3.connect('C:\Temp\lab1.db')
    Lite_cur = Lite_con.cursor()
    # Створення підключення до БД postgreSQL
    postgres_con = psycopg2.connect(host="localhost", user="postgres", password="DjangoLab", dbname="djangolab1")
    postgres_cur = postgres_con.cursor()
    # Створення нової таблиці абонентів в postgreSQL
    # Передається 3 змінні, та телефонний номер заміняє ID як основний ключ
    cur_time = datetime.datetime.now()
    table_name = "subscriptions_"+cur_time.strftime("%d_%m_%Y_%H_%M_%S")
    table_create_command = '''CREATE TABLE '''+table_name+'''
                            (telephone_number TEXT PRIMARY KEY, tariff_type TEXT, paid_for INTEGER)'''
    postgres_cur.execute(table_create_command)
    # Значення з БД1 (окрім ID) записуються в БД2
    Lite_cur.execute("SELECT telephone_number, tariff_type, paid_for FROM subscriptions")
    entry_list_db1 = Lite_cur.fetchall()
    postgres_cur.executemany("INSERT INTO "+table_name+" VALUES (%s, %s, %s)", entry_list_db1)
    # Закінчення транзакції postgreSQL
    postgres_con.commit()
    # Закінчення транзакції SQLite
    Lite_con.commit()

    # Створення підключення до БД3 MySQL
    mysql_con = mysql.connector.connect(host="localhost", user="Python_app", password="DjangoLab", database='djangolab1')
    mysql_cur = mysql_con.cursor()
    # Створення нової таблиці абонентів в MySQL
    table_create_command = '''CREATE TABLE '''+table_name+'''
                            (telephone_number VARCHAR(17) PRIMARY KEY, tariff_type TEXT, paid_for INTEGER)'''
    mysql_cur.execute(table_create_command)
    # Створення повного списку значень БД2 для текстового виведення
    postgres_cur.execute("SELECT * FROM "+table_name)
    entry_list_db2 = postgres_cur.fetchall()
    # Значення для БД3 обираються з додатковою умовою - в БД3 йдуть тільки абоненти з оплаченими тарифами
    postgres_cur.execute("SELECT * FROM "+table_name+" WHERE paid_for = 1")
    entry_list_db2_for3 = postgres_cur.fetchall()
    postgres_con.commit()
    # Значення з БД2 записуються в БД3
    mysql_cur.executemany("INSERT INTO "+table_name+" VALUES (%s, %s, %s)", entry_list_db2_for3)
    # Над значеннями БД3 виконується додаткова трасформація
    mysql_cur.execute('''UPDATE '''+table_name+'''  
    SET telephone_number = Concat('+381 ', Substring(telephone_number, 6, 11)), 
        tariff_type = Concat(tariff_type, ' 2021'),
        paid_for = 1 - paid_for
    ''')
    # Створення повного списку значень БД3 для текстового виведення
    mysql_cur.execute("SELECT * FROM "+table_name)
    entry_list_db3 = mysql_cur.fetchall()
    # Закінчення транзакції MySQL
    mysql_con.commit()

    # Закриття підключень
    postgres_con.close()
    mysql_con.close()
    Lite_con.close()

    # Виведення результатів в консоль
    print("БД2 - postgreSQL:")
    for i in entry_list_db2:
        print(i)
    print("БД3 - MySQL:")
    for i in entry_list_db3:
        print(i)

    # Повертає значення з БД2 та БД3 для текстового показу
    return entry_list_db2, entry_list_db3