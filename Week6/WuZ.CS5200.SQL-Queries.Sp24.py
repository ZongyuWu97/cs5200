import sqlite3

try:
    # 1
    dbcon = sqlite3.connect("MediaDB.db")
    cursor = dbcon.cursor()
    print("connection successful")

    sql = "SELECT name FROM sqlite_master WHERE type='table';"
    cursor.execute(sql)
    rs = cursor.fetchall()
    print("\n\nTask1\n", rs)

    # 2
    sql = "SELECT LastName, FirstName, Title, HireDate FROM employees;"
    cursor.execute(sql)
    rs = cursor.fetchall()
    print("\n\nTask2\n", rs)

    # 3
    sql = "select sum(Bytes) from tracks"
    cursor.execute(sql)
    rs = cursor.fetchall()
    print("\n\nTask3\n", rs)

    # 4
    sql = "select * from genres"
    cursor.execute(sql)
    rs = cursor.fetchall()
    print("\n\nTask4\n", rs)

    cursor.close()

except sqlite3.Error as error:
    print("can't connect", error)

finally:
    if dbcon:
      dbcon.commit()
      dbcon.close()
      print("connection closed")
