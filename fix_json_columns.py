import sqlite3
import json
import os

def fix_json():
    db_path = "vcdp.db"
    if not os.path.exists(db_path):
        print(f"Database {db_path} not found.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # List of columns that should be JSON arrays (list)
    json_columns = [
        "commodity", "vcdp_sub_components", "lgas", 
        "threeFS_primary", "threeFS_sub_components", 
        "funding_sources", "sub_funding_sources", 
        "beneficiary_categories", "value_chain_segments", 
        "data_source", "supporting_documents"
    ]
    
    cursor.execute(f"SELECT id, {', '.join(json_columns)} FROM transactions")
    rows = cursor.fetchall()
    
    updates = []
    for row in rows:
        row_id = row[0]
        new_values = {}
        for i, col_name in enumerate(json_columns):
            val = row[i+1]
            try:
                if val is None:
                    new_val = "[]"
                elif isinstance(val, str):
                    if val.strip() == "":
                        new_val = "[]"
                    else:
                        # Try to parse as JSON
                        json.loads(val)
                        new_val = val # It's already valid JSON
                else:
                    new_val = json.dumps([val]) # Shouldn't happen but safe
            except (json.JSONDecodeError, TypeError):
                # If it fails, it's likely a plain string or something. Wrap it in a list.
                new_val = json.dumps([val])
                print(f"Repairing record {row_id} column {col_name}: {val} -> {new_val}")
            
            if new_val != val:
                new_values[col_name] = new_val
        
        if new_values:
            updates.append((row_id, new_values))
    
    for row_id, vals in updates:
        set_clause = ", ".join([f"{k} = ?" for k in vals.keys()])
        cursor.execute(f"UPDATE transactions SET {set_clause} WHERE id = ?", list(vals.values()) + [row_id])
    
    conn.commit()
    conn.close()
    print(f"Repaired {len(updates)} records.")

if __name__ == "__main__":
    fix_json()
