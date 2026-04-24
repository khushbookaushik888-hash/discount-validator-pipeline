import csv
import sqlite3
from datetime import datetime

def run_discount_pipeline():
    # --- STEP 1: EXTRACT (Data ko read karna) ---
    print("Step 1: Reading data from CSV files...")
    
    # Rules ko dictionary mein store karna (Easy lookup ke liye)
    rules = {}
    try:
        with open('coupon_rules.csv', mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                rules[row['coupon_code']] = row
    except FileNotFoundError:
        print("Error: 'coupon_rules.csv' nahi mili!")
        return

    # Orders ko list mein store karna
    orders = []
    try:
        with open('applied_coupons.csv', mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                orders.append(row)
    except FileNotFoundError:
        print("Error: 'applied_coupons.csv' nahi mili!")
        return

    # --- STEP 2: TRANSFORM (Rules validate karna aur calculation) ---
    print(f"Step 2: Processing {len(orders)} orders...")
    final_processed_data = []

    for order in orders:
        order_id = order['order_id']
        price = float(order['original_price'])
        coupon = order['coupon_code']
        category = order['category']
        order_date = order['order_date']

        status = "REJECTED" # By default rejected rakhenge
        reason = "Valid"
        discount_amount = 0.0

        # Rule validation logic
        if coupon not in rules:
            reason = "Invalid Coupon Code"
        else:
            rule = rules[coupon]
            
            # A. Expiry Check
            if order_date > rule['expiry_date']:
                reason = "Coupon Expired"
            
            # B. Category Check
            elif category != rule['category']:
                reason = "Category Mismatch"
            
            # C. Agar sab sahi hai toh Discount Calculate karo
            else:
                status = "ACCEPTED"
                if rule['type'] == 'percentage':
                    discount_amount = price * (float(rule['value']) / 100)
                else:
                    discount_amount = float(rule['value'])

        final_price = price - discount_amount
        
        # Ek row ka final result
        final_processed_data.append((
            order_id, coupon, price, discount_amount, final_price, status, reason
        ))

    # --- STEP 3: LOAD (Database mein save karna) ---
    print("Step 3: Saving results to SQLite Database...")
    conn = sqlite3.connect('discount_report.db')
    cursor = conn.cursor()

    # Purani table delete karke nayi banana
    cursor.execute('DROP TABLE IF EXISTS validation_results')
    cursor.execute('''
        CREATE TABLE validation_results (
            OrderID TEXT, 
            CouponCode TEXT, 
            OriginalPrice REAL, 
            Discount REAL, 
            FinalPrice REAL, 
            Status TEXT, 
            Remarks TEXT
        )
    ''')

    # Bulk insert (Tezi se data dalne ke liye)
    cursor.executemany('INSERT INTO validation_results VALUES (?,?,?,?,?,?,?)', final_processed_data)
    
    conn.commit()
    conn.close()

    # --- FINAL SUMMARY (Presentation ke liye) ---
    print("\n" + "="*30)
    print("PIPELINE EXECUTION COMPLETE")
    print("="*30)
    accepted = sum(1 for r in final_processed_data if r[5] == "ACCEPTED")
    print(f"Total Rows Processed : {len(final_processed_data)}")
    print(f"Coupons Accepted     : {accepted}")
    print(f"Orders Rejected      : {len(final_processed_data) - accepted}")
    print(f"Database Saved as    : 'discount_report.db'")
    print("="*30)

# Pipeline ko chalane ke liye function call
if __name__ == "__main__":
    run_discount_pipeline()