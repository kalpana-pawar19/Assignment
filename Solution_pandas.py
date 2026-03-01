import sqlite3
import pandas as pd

conn = sqlite3.connect("database.db")

customers = pd.read_sql("SELECT * FROM customers", conn)
orders = pd.read_sql("SELECT * FROM orders", conn)
order_items = pd.read_sql("SELECT * FROM order_items", conn)

df = customers.merge(orders, on="customer_id")
df = df.merge(order_items, on="order_id")

df = df[(df["age"] >= 18) & (df["age"] <= 35)]

df["quantity"] = df["quantity"].fillna(0)

result = (
    df.groupby(["customer_id", "age", "item"])["quantity"]
    .sum()
    .reset_index()
)

result = result[result["quantity"] > 0]

result["quantity"] = result["quantity"].astype(int)

result.columns = ["Customer", "Age", "Item", "Quantity"]

result.to_csv("output_pandas.csv", sep=";", index=False)

conn.close()