import sqlite3

conn = sqlite3.connect('discount_report.db')
cursor = conn.cursor()

print("\n--- Project Audit: Top 5 Rejected Orders ---")
cursor.execute("SELECT OrderID, Coupon, Remarks FROM validation_results WHERE Status = 'REJECTED' LIMIT 5")
for row in cursor.fetchall():
    print(f"Order: {row[0]} | Coupon: {row[1]} | Reason: {row[2]}")

print("\n--- Project Audit: Total Revenue Saved (Blocked Discounts) ---")
cursor.execute("SELECT SUM(Discount) FROM validation_results WHERE Status = 'REJECTED'")
total_saved = cursor.fetchone()[0]
print(f"Total Money Saved from Invalid Coupons: Rs. {total_saved:.2f}")

conn.close()