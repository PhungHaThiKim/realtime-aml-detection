import pandas as pd
import pymssql

DATASET_PATH = "../dataset/LI-Small_Trans.csv"

# Chỉ đọc 1000 dòng đầu tiên
df = pd.read_csv(DATASET_PATH, nrows=1000)

conn = pymssql.connect(
    server="localhost", user="sa", password="YourStrong!Passw0rd", database="master"
)
conn.autocommit(True)  # 👈 Thêm dòng này
cursor = conn.cursor()


def init_database():
    # Lấy các giá trị unique của Payment Format
    payment_formats = df["Payment Format"].unique()
    # Lặp qua từng format
    for payment_format in payment_formats:
        db_name = f"Transactions_{payment_format.replace(' ', '_')}"

        print(f"Recreating database: {db_name}")

        # Xóa database nếu đã tồn tại
        cursor.execute(
            f"""
        IF EXISTS (SELECT name FROM sys.databases WHERE name = '{db_name}')
        BEGIN
            ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE [{db_name}]
        END
        """
        )

        # Tạo lại database
        cursor.execute(f"CREATE DATABASE [{db_name}]")


def create_table_and_insert_data():
    for payment_format in df["Payment Format"].unique():
        db_name = f"Transactions_{payment_format.replace(' ', '_')}"
        print(f"Connecting to database: {db_name}")

        # Kết nối vào DB tương ứng
        conn_db = pymssql.connect(
            server="localhost",
            user="sa",
            password="YourStrong!Passw0rd",
            database=db_name,
        )
        conn_db.autocommit(True)
        cursor_db = conn_db.cursor()

        # Tạo bảng Transactions nếu chưa có
        cursor_db.execute(
            """
        IF OBJECT_ID('Transactions', 'U') IS NULL
        CREATE TABLE Transactions (
            [Timestamp] DATETIME,
            [FromBank] NVARCHAR(50),
            [FromAccount] NVARCHAR(50),
            [ToBank] NVARCHAR(50),
            [ToAccount] NVARCHAR(50),
            [AmountReceived] FLOAT,
            [ReceivingCurrency] NVARCHAR(50),
            [AmountPaid] FLOAT,
            [PaymentCurrency] NVARCHAR(50),
            [PaymentFormat] NVARCHAR(50),
            [IsLaundering] BIT
        )
        """
        )

        # Lọc dữ liệu theo format này
        subset = df[df["Payment Format"] == payment_format]

        # Insert từng dòng
        for _, row in subset.iterrows():
            cursor_db.execute(
                """
                INSERT INTO Transactions VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    pd.to_datetime(row["Timestamp"]),
                    str(row["From Bank"]),
                    row["Account"],
                    str(row["To Bank"]),
                    row["Account.1"],
                    float(row["Amount Received"]),
                    row["Receiving Currency"],
                    float(row["Amount Paid"]),
                    row["Payment Currency"],
                    row["Payment Format"],
                    int(row["Is Laundering"]),
                ),
            )

        print(f"Inserted {len(subset)} rows into {db_name}.Transactions")
        conn_db.close()


if __name__ == "__main__":
    init_database()
    create_table_and_insert_data()
    cursor.close()
    conn.close()
