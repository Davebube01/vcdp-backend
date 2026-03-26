import sqlite3
import os

def migrate():
    db_path = "vcdp.db"
    if not os.path.exists(db_path):
        print(f"Database {db_path} not found.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create documents table
    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id VARCHAR PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            filename VARCHAR(300) NOT NULL,
            file_path VARCHAR(500) NOT NULL,
            state VARCHAR(100) NOT NULL,
            data_source VARCHAR(100) NOT NULL,
            uploaded_by VARCHAR,
            uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(uploaded_by) REFERENCES users (id)
        )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS ix_documents_state ON documents (state)")
        cursor.execute("CREATE INDEX IF NOT EXISTS ix_documents_data_source ON documents (data_source)")
        
        print("Documents table and indexes created successfully.")
    except Exception as e:
        print(f"Error creating documents table: {e}")
        
    conn.commit()
    conn.close()

if __name__ == "__main__":
    migrate()
