import sqlite3

def inspect_db():
    conn = sqlite3.connect('vcdp.db')
    cursor = conn.cursor()
    
    print("--- Users Table ---")
    try:
        cursor.execute("SELECT id, email, hashed_password FROM users")
        rows = cursor.fetchall()
        for row in rows:
            print(f"ID: {row[0]}, Email: {row[1]}, Hash Length: {len(row[2]) if row[2] else 'None'}")
            print(f"Hash Start: {row[2][:10] if row[2] else 'N/A'}...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    inspect_db()
