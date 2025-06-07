from pymysql import Connection

# 标志常量 -------------------------------------------------------------------------------------
INSERT = 0x0000  # 数据表操作方式-插入
UPDATE = 0x0001  # 数据表操作方式-修改
DELETE = 0x0002  # 数据表操作方式-删除


# 初始操作函数 ---------------------------------------------------------------------------------
def linked_database():
    """
    获取到MySQL数据库的链接对象
    """
    my_conn = Connection(
        host='localhost',  # 主机地址
        port=3306,  # 连接端口
        user='root',  # 用户名
        password='123456'  # 用户密码
    )
    return my_conn


def init(cursor, conn):
    """
    初始化StudentAttendanceChart数据库
    如果存在则删除并新建数据库结构
    如果不存在则新建数据库结构
    :param cursor: 外部获取的游标对象
    :param conn: 外部获取的Connection对象
    """
    try:
        # 关闭自动提交
        cursor.execute("SET @@autocommit = 0;")

        # 检查数据库是否存在
        cursor.execute(
            """
            SELECT COUNT(*)
                FROM information_schema.SCHEMATA
                WHERE SCHEMA_NAME = 'student_attendance_chart';
            """
        )
        data_status = cursor.fetchone()  # 返回数据库存在状态结果

        if data_status[0] == 1:  # 如果数据库存在
            # 删除数据库
            cursor.execute("DROP DATABASE student_attendance_chart;")

        # 创建数据库
        cursor.execute(
            """
            CREATE DATABASE student_attendance_chart
                DEFAULT CHARACTER SET utf8mb4
                DEFAULT COLLATE utf8mb4_0900_ai_ci;
            """
        )
        cursor.execute("USE student_attendance_chart;")  # 开始操作该数据库
        # 创建所有表
        create_class_table(cursor)  # 创建班级信息表
        create_counselor_table(cursor)  # 创建辅导员信息表
        create_student_table(cursor)  # 创建学生信息表
        create_attendance_table(cursor)  # 创建考勤信息表
        create_attendance_copy_table(cursor)  # 创建考勤信息表_副表
        # 创建存储过程
        create_pro_insert_studentData(cursor)  # 创建插入学生信息存储过程
        # 创建触发器
        create_tr_attendance_insert(cursor)  # 创建考勤记录表插入前触发器
        # 创建视图
        create_user_view(cursor)  # 创建用户视图

        # 提交事务
        conn.commit()

    except Exception as e:
        # 出错回滚
        conn.rollback()
        raise e
    finally:
        # 恢复自动提交（可选）
        cursor.execute("SET @@autocommit = 1;")


# 初始化数据库构建语句函数 -----------------------------------------------------------------------
# table ---------------------------------------------------
def create_class_table(cursor):
    """
    创建班级信息表
    :param cursor: 外部获取的游标对象
    """
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Class (
            classId INT COMMENT '班级索引号' PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(25) COMMENT '班级名称' NOT NULL
        ) ENGINE = InnoDB COMMENT '班级信息表';
        """
    )


def create_counselor_table(cursor):
    """
    创建辅导员信息表
    :param cursor: 外部获取的游标对象
    """
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Counselor (
            counselorId INT COMMENT '辅导员索引号' PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(25) COMMENT '辅导员姓名' NOT NULL
        ) ENGINE = InnoDB COMMENT '辅导员信息表';
        """
    )


def create_student_table(cursor):
    """
    创建学生信息表
    :param cursor: 外部获取的游标对象
    """
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Student (
            stuId CHAR(9) COMMENT '学生学号' PRIMARY KEY,  
            name VARCHAR(20) COMMENT '学生姓名' NOT NULL,
            classId INT COMMENT '班级索引号' NOT NULL,
            counselorId INT COMMENT '辅导员索引号' NOT NULL,
            totalNum INT COMMENT '总计旷课节数' DEFAULT 0,
            CONSTRAINT fr_Class_Student_classId
                FOREIGN KEY (classId) REFERENCES Class(classId)
                ON UPDATE CASCADE
                ON DELETE CASCADE,
            CONSTRAINT fr_Counselor_Student_counselorId
                FOREIGN KEY (counselorId) REFERENCES Counselor(counselorId)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        ) ENGINE = InnoDB COMMENT '学生信息表';
        """
    )


def create_attendance_table(cursor):
    """
    创建考勤信息表
    :param cursor: 外部获取的游标对象
    """
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Attendance (
            stuId CHAR(9) COMMENT '学生学号' PRIMARY KEY,
            date VARCHAR(10) COMMENT '考勤起始日期' NOT NULL,
            Mon VARCHAR(25) COMMENT '星期一' DEFAULT '',
            Tue VARCHAR(25) COMMENT '星期二' DEFAULT '',
            Wed VARCHAR(25) COMMENT '星期三' DEFAULT '',
            Thu VARCHAR(25) COMMENT '星期四' DEFAULT '',
            Fri VARCHAR(25) COMMENT '星期五' DEFAULT '',
            Sat VARCHAR(25) COMMENT '星期六' DEFAULT '',
            Sun VARCHAR(25) COMMENT '星期日' DEFAULT '',
            remarks VARCHAR(100) COMMENT '备注' DEFAULT '', 
            CONSTRAINT fr_Student_Attendance_stuId
                FOREIGN KEY (stuId) REFERENCES Student(stuId)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        ) ENGINE = InnoDB COMMENT '考勤信息表';
        """
    )


def create_attendance_copy_table(cursor):
    """
    创建考勤信息表_副表
    :param cursor: 外部获取的游标对象
    """
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Attendance_copy (
            stuId CHAR(9) COMMENT '学生学号' PRIMARY KEY,
            date VARCHAR(10) COMMENT '考勤起始日期' NOT NULL,
            Mon VARCHAR(25) COMMENT '星期一' DEFAULT '',
            Tue VARCHAR(25) COMMENT '星期二' DEFAULT '',
            Wed VARCHAR(25) COMMENT '星期三' DEFAULT '',
            Thu VARCHAR(25) COMMENT '星期四' DEFAULT '',
            Fri VARCHAR(25) COMMENT '星期五' DEFAULT '',
            Sat VARCHAR(25) COMMENT '星期六' DEFAULT '',
            Sun VARCHAR(25) COMMENT '星期日' DEFAULT '',
            remarks VARCHAR(100) COMMENT '备注' DEFAULT '', 
            CONSTRAINT fr_Student_Attendance_copy_stuId
                FOREIGN KEY (stuId) REFERENCES Student(stuId)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        ) ENGINE = InnoDB COMMENT '考勤信息表_副表';
        """
    )


# view -----------------------------------------------------
def create_user_view(cursor):
    """
    创建用户视图
    :param cursor: 外部获取的游标对象
    """
    cursor.execute(
        """
        CREATE OR REPLACE VIEW user_view(学号, 姓名, 班级, 周一, 周二, 周三, 周四, 周五, 周六, 周日, 本周节数, 共计节数, 辅导员, 备注)
         AS
         SELECT s.stuId AS 学号, s.name AS 姓名, c.name AS 班级, a.Mon AS 周一, a.Tue AS 周二, a.Wed AS 周三, a.Thu AS 周四, a.Fri AS 周五, a.Sat AS 周六, a.Sun AS 周日, COUNT(*) AS 本周节数, s.totalNum + COUNT(*) AS 共计节数, co.name AS 辅导员, a.remarks AS 备注
           FROM Class AS c, Student AS s, Counselor AS co, Attendance AS a
           WHERE
             c.classId = s.classId
             AND
             co.counselorId = s.counselorId
             AND
             s.stuId = a.stuId
           GROUP BY a.stuId;
        """
    )


# procedure ---------------------------------------------
def create_pro_insert_studentData(cursor):
    """
    创建插入学生信息存储过程
    功能：插入学生信息
    :param cursor: 外部获取的游标对象
    """
    procedure_sql = """
        CREATE PROCEDURE pro_insert_studentData (
            IN studentId VARCHAR(9),
            IN studentName VARCHAR(20),
            IN className VARCHAR(25),
            IN counselorName VARCHAR(25)
        )
        BEGIN 
            DECLARE clsId INT;
            DECLARE couId INT;
            SELECT classId INTO clsId FROM Class WHERE name = className;
            SELECT counselorId INTO couId FROM Counselor WHERE  name = counselorName;
            INSERT INTO Student(stuId, name, classId, counselorId)
                VALUES (studentId, studentName, clsId, couId);
        END
        """
    try:
        cursor.execute(procedure_sql)
    except Exception as e:
        print(f"[ERROR] 创建存储过程失败：{e}")
        raise


# trigger -----------------------------------------------
def create_tr_attendance_insert(cursor):
    """
    创建考勤记录表插入前触发器
    功能：确保学生、班级、辅导员存在，若不存在则自动创建
    :param cursor: 外部获取的游标对象
    """
    trigger_sql = """
    CREATE TRIGGER attendance_insert
    BEFORE INSERT ON attendance
    FOR EACH ROW
    BEGIN
        DECLARE student_exists INT DEFAULT 0;

        -- 检查学生是否存在
        SELECT COUNT(*) INTO student_exists FROM student WHERE stuId = NEW.stuId;

        -- 如果学生不存在，则尝试创建学生记录
        IF student_exists = 0 THEN
            -- 学生不存在，抛出异常通知业务层
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = '学生不存在，请先创建学生信息';
        # ELSE
        #     -- 存在则更新旷课次数
        #     UPDATE student
        #         SET totalNum = totalNum + 1
        #         WHERE stuId = NEW.stuId;
        END IF;
        INSERT INTO attendance_copy(stuId, date, Mon, Tue, Wed, Thu, Fri, Sat, Sun, remarks)
            VALUES (NEW.stuId, NEW.date, NEW.Mon, NEW.Tue, NEW.Wed, NEW.Thu, NEW.Fri, NEW.Sat, NEW.Sun, NEW.remarks);
    END;
    """

    try:
        cursor.execute(trigger_sql)
    except Exception as e:
        print(f"[ERROR] 创建触发器失败：{e}")
        raise


# 数据表操作语句 -------------------------------------------------------------------------------
# 数据添加语句函数
def operation_ClassData(cursor, operation_typer, *data):
    """
    添加班级信息
    :param cursor: 外部获取的游标对象
    :param operation_typer: 数据表操作类型
    :param data: 存放班级名称的元组数据容器
    """
    # 从data中依次获取班级名称
    if operation_typer == INSERT:
        for className in data:
            cursor.execute(
                """
                INSERT INTO Class(name)
                    VALUES (%s);
                """,
                className
            )
    elif operation_typer == UPDATE:
        cursor.execute(
            """
            UPDATE Class SET name = %s
                WHERE name = %s;
            """,
            data[1],
            data[0]
        )
    elif operation_typer == DELETE:
        for className in data:
            cursor.execute(
                """
                DELETE FROM Class
                    WHERE name = %s;
                """,
                className
            )


def operation_CounselorData(cursor, operation_typer, *data):
    """
    添加辅导员信息
    :param cursor: 外部获取的游标对象
    :param operation_typer: 数据表操作类型
    :param data: 存放辅导员姓名的元组数据容器
    """
    if operation_typer == INSERT:
        for counselorName in data:
            cursor.execute(
                """
                INSERT INTO Counselor(name)
                    VALUES (%s);
                """,
                counselorName
            )
    elif operation_typer == UPDATE:
        cursor.execute(
            """
            UPDATE Counselor SET name = %s
                WHERE name = %s;
            """,
            data[1],
            data[0]
        )
    elif operation_typer == DELETE:
        for counselorName in data:
            cursor.execute(
                """
                DELETE FROM Counselor
                    WHERE name =  %s;
                """,
                counselorName
            )


def operation_StudentData(cursor, operation_typer, **data):
    """
    添加数据
    :param cursor: 外部获取的游标对象
    :param operation_typer: 数据表操作类型
    :param data: 添加的数据({})
    """
    if operation_typer == INSERT:
        cursor.callproc('pro_insert_studentData', (
            data["stuId"],
            data["name"],
            data["class"],
            data["counselor"]
        ))
    elif operation_typer == DELETE:
        cursor.execute(
            """
            DELETE FROM student WHERE stuId = %s;
            """,
            data["stuId"]
        )
