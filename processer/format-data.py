import pandas as pd

# Đọc file gốc
df = pd.read_csv("../neo4j/import/LI-Small_Trans.csv")

# Đổi tên cột 'Account' ở vị trí đầu thành 'From Account', còn cột sau là 'To Account'
df = df.rename(columns={df.columns[2]: "From Account", df.columns[4]: "To Account"})

# Ghi ra file mới
df.to_csv("../neo4j/import/LI-Small_Trans_clean.csv", index=False)

print("✅ Đã tạo file LI-Small_Trans_clean.csv với cột From Account / To Account.")
