import sqlite3
import os

def migrate():
    db_path = "vcdp.db"
    if not os.path.exists(db_path):
        print(f"Database {db_path} not found.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check current columns to avoid redundant errors
    cursor.execute("PRAGMA table_info(transactions)")
    columns = [row[1] for row in cursor.fetchall()]
    
    if "sub_funding_sources" not in columns:
        try:
            # SQLAlchemy JSON maps to TEXT in SQLite
            cursor.execute("ALTER TABLE transactions ADD COLUMN sub_funding_sources TEXT DEFAULT '[]'")
            print("Added sub_funding_sources column")
        except Exception as e:
            print(f"Error adding sub_funding_sources: {e}")
    else:
        print("sub_funding_sources column already exists")

    if "expenditure_total_reported" not in columns:
        try:
            cursor.execute("ALTER TABLE transactions ADD COLUMN expenditure_total_reported FLOAT DEFAULT 0.0")
            print("Added expenditure_total_reported column")
        except Exception as e:
            print(f"Error adding expenditure_total_reported: {e}")
    else:
        print("expenditure_total_reported column already exists")
        
    conn.commit()
    conn.close()
    print("Migration completed.")

if __name__ == "__main__":
    migrate()
