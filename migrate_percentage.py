import sqlite3

def run():
    conn = sqlite3.connect("vcdp.db")
    cursor = conn.cursor()
    columns_to_add = [
        "beneficiary_male_percentage REAL DEFAULT 0.0",
        "beneficiary_female_percentage REAL DEFAULT 0.0",
        "beneficiary_youth_percentage REAL DEFAULT 0.0"
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
