from pymysql import Connection, MySQLError
import sic

# 测试示例数据元组
test_data = (
    {
        "stdId": "240200401",
        "name": "陈世楷",
        "class": "24大数据4",
        "data": "5.26",
        "Mon": "1.2.3.4",
        "Tue": "1.4",
        "Wed": "1.2.4",
        "Thu": "1.2",
        "Fri": "",
        "Sat": "1.2",
        "Sun": "3.4",
        "totalNum": 0,
        "counselor": "杨凯烽",
        "remarks": "无"
    },
    {
        "stdId": "240200402",
        "name": "黄逊麾",
        "class": "24大数据4",
        "data": "5.26",
        "Mon": "4",
        "Tue": "1.4",
        "Wed": "1",
        "Thu": "1.2",
        "Fri": "",
        "Sat": "1.2",
        "Sun": "",
        "totalNum": 0,
        "counselor": "杨凯烽",
        "remarks": "无敌"
    }
)

# 正式程序
conn = sic.linked_database()
cursor = conn.cursor()

try:
    # 初始化数据库结构
    sic.init(cursor, conn)

    # 插入测试数据
    for data in test_data:
        stu_id = data["stdId"]
        try:
            cursor.execute("""
                INSERT INTO attendance(stuId, date, Mon, Tue, Wed, Thu, Fri, Sat, Sun, remarks)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                stu_id,
                data["data"],
                data["Mon"], data["Tue"], data["Wed"], data["Thu"],
                data["Fri"], data["Sat"], data["Sun"], data["remarks"]
            ))
            conn.commit()
        except MySQLError as e:
            if e.args[0] == 1644:  # 学生不存在的错误码
                print(f"[INFO] 学生 {stu_id} 不存在，正在创建...")

                # 创建班级和辅导员（如已存在则忽略）
                try:
                    cursor.execute("SELECT COUNT(*) FROM Class WHERE name = %s", data["class"])
                    if cursor.fetchone()[0] == 0:
                        print(f"[INFO] 班级 {data["class"]} 不存在，正在创建...")
                        sic.operation_ClassData(cursor, sic.INSERT, data["class"])
                        print(f"[SUCCESS] 成功创建班级 {data['class']}")

                    cursor.execute("SELECT COUNT(*) FROM counselor WHERE name = %s", data["counselor"])
                    if cursor.fetchone()[0] == 0:
                       print(f"[INFO] 辅导员 {data['counselor']} 不存在，正在创建...")
                       sic.operation_CounselorData(cursor, sic.INSERT, data["counselor"])
                       print(f"[SUCCESS] 成功创建辅导员 {data['counselor']}")
                except Exception as inner_e:
                    print(f"[ERROR] 创建班级/辅导员失败：{inner_e}")
                    conn.rollback()
                    continue

                # 创建学生
                try:
                    stu_data = {"stuId": stu_id, "name": data["name"], "class": data["class"], "counselor": data["counselor"]}
                    sic.operation_StudentData(cursor, sic.INSERT, **stu_data)
                except Exception as inner_e:
                    print(f"[ERROR] 创建学生失败：{inner_e}")
                    conn.rollback()
                    continue

                # 再次尝试插入考勤记录
                try:
                    cursor.execute("""
                        INSERT INTO attendance(stuId, date, Mon, Tue, Wed, Thu, Fri, Sat, Sun, remarks)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        stu_id,
                        data["data"],
                        data["Mon"], data["Tue"], data["Wed"], data["Thu"],
                        data["Fri"], data["Sat"], data["Sun"], data["remarks"]
                    ))
                    conn.commit()
                    print(f"[SUCCESS] 成功插入考勤记录（学生 {stu_id} 已自动创建）")
                except Exception as retry_e:
                    print(f"[ERROR] 第二次插入考勤记录失败：{retry_e}")
                    conn.rollback()
            else:
                print(f"[ERROR] 发生未知数据库错误：{e}")
                conn.rollback()

finally:
    cursor.close()
    conn.close()
