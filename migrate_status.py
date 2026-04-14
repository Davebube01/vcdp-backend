import sqlite3

def run():
    conn = sqlite3.connect("vcdp.db")
    cursor = conn.cursor()
    columns_to_add = [
        "status VARCHAR(50) DEFAULT 'PUBLISHED'",
        "rejection_reason TEXT"
    ]
    for col in columns_to_add:
        try:
            cursor.execute(f"ALTER TABLE transactions ADD COLUMN {col}")
            print(f"Added {col}")
        except Exception as e:
            print(f"Error adding {col}: {e}")
            
    conn.commit()
    conn.close()

if __name__ == "__main__":
    run()
