import pandas as pd

# Đọc dữ liệu
df = pd.read_csv("../neo4j/import/LI-Small_Trans_clean.csv")

# Lấy các chỉ số index có laundering
laundering_indices = df[df["Is Laundering"] == 1].index

# Tập hợp các chỉ số cần giữ lại
selected_indices = set()
print(len(laundering_indices))
laundering_indices = laundering_indices[:100]
print(len(laundering_indices))
for idx in laundering_indices:
    start = max(0, idx - 100)
    end = min(len(df), idx + 5 + 1)  # +1 vì slicing là exclusive
    selected_indices.update(range(start, end))

# Chuyển về danh sách và sort lại theo thứ tự gốc
final_indices = sorted(selected_indices)

# Trích xuất các dòng tương ứng
context_df = df.loc[final_indices]

# Ghi ra file mới
context_df.to_csv("../neo4j/import/laundering_with_context.csv", index=False)

print(f"✅ Đã xuất {len(context_df)} dòng vào 'laundering_with_context.csv'")
