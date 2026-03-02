import sqlite3

def inspect_transactions():
    conn = sqlite3.connect("vcdp.db")
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, ref_id, project_name FROM transactions LIMIT 5")
    rows = cursor.fetchall()
    
    print("--- Transactions Table ---")
    for row in rows:
        print(f"ID: {row[0]}, Ref: {row[1]}, Name: {row[2]}")
    
    conn.close()

if __name__ == "__main__":
    inspect_transactions()
