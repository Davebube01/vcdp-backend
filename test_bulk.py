import pandas as pd
import sys
import os

# Adjust sys path so we can import from app
sys.path.append(os.path.abspath('.'))

from app.schemas import TransactionCreate
from app.models import Transaction, TransactionStatus, Currency

def safe_float(val):
    if pd.isna(val) or val == "": return 0.0
    try: return float(val)
    except: return 0.0

def safe_int(val):
    if pd.isna(val) or val == "": return None
    try: return int(float(val))
    except: return None

def safe_str(val):
    if pd.isna(val): return None
    return str(val).strip()

def safe_list(val):
    if pd.isna(val) or not str(val).strip(): return []
    val_str = str(val).strip()
    if val_str.startswith('[') and val_str.endswith(']'):
        import json
        try: return json.loads(val_str)
        except: pass
    return [item.strip() for item in val_str.split(',') if item.strip()]

def test_upload():
    file_path = '../excel/Test_Bulk_Upload_Populated.xlsx'
    try:
        xls = pd.ExcelFile(file_path)
    except Exception as e:
        print(f"File Error: {e}")
        return

    errors = []
    VALID_STATE_TABS = [
        'Anambra', 'Benue', 'Ebonyi', 'Enugu', 'Kogi', 
        'Nasarawa', 'Niger', 'Ogun', 'Taraba'
    ]

    for sheet_name in xls.sheet_names:
        if sheet_name not in VALID_STATE_TABS:
            continue
            
        df = pd.read_excel(xls, sheet_name=sheet_name)
        if df.empty: continue

        for index, row in df.iterrows():
            row_num = index + 2
            project_name = safe_str(row.get("Project / Activity Name"))
            if not project_name: continue

            try:
                q1 = safe_float(row.get("Quantity Q1"))
                q2 = safe_float(row.get("Quantity Q2"))
                q3 = safe_float(row.get("Quantity Q3"))
                q4 = safe_float(row.get("Quantity Q4"))
                
                quarterly_data = {}
                if q1 > 0: quarterly_data["Q1"] = {"total": q1, "male": 0, "female": 0, "youth_under35": 0, "plwd": 0}
                if q2 > 0: quarterly_data["Q2"] = {"total": q2, "male": 0, "female": 0, "youth_under35": 0, "plwd": 0}
                if q3 > 0: quarterly_data["Q3"] = {"total": q3, "male": 0, "female": 0, "youth_under35": 0, "plwd": 0}
                if q4 > 0: quarterly_data["Q4"] = {"total": q4, "male": 0, "female": 0, "youth_under35": 0, "plwd": 0}
                
                raw_status = safe_str(row.get("Status"))
                status = TransactionStatus.PUBLISHED
                if raw_status:
                    try: status = TransactionStatus(raw_status)
                    except: pass
                        
                currency_val = safe_str(row.get("Currency"))
                currency = "NGN"
                if currency_val and currency_val.upper() == "USD":
                    currency = "USD"

                record_data = {
                    "ref_id": safe_str(row.get("Ref ID")) or "",
                    "project_name": project_name,
                    "activity_type_code": safe_str(row.get("Activity Code")),
                    "record_type": safe_str(row.get("Record Type")) or "Actual",
                    "status": status,
                    "institution_code": safe_str(row.get("Institution Code")),
                    "executing_agency": safe_str(row.get("Executing Agency")),
                    
                    "state": sheet_name,
                    "lgas": safe_list(row.get("LGA(s)")),
                    
                    "fy_awarded": safe_int(row.get("FY Awarded")),
                    "fy_completed": safe_int(row.get("FY Completed")),
                    "programme_phase": safe_str(row.get("Programme Phase")),
                    "fiscal_quarter": safe_list(row.get("Fiscal Quarter")),
                    
                    "commodity": safe_list(row.get("Commodity")),
                    "vcdp_component": safe_list(row.get("VCDP Component")),
                    "vcdp_sub_components": safe_list(row.get("VCDP Sub-Component(s)")),
                    "threeFS_primary": safe_list(row.get("3FS Primary Component")),
                    "threeFS_sub_components": safe_list(row.get("3FS Sub-Component(s)")),
                    
                    "cofog_code": safe_str(row.get("COFOG Code")),
                    "cofog_divisions": safe_list(row.get("COFOG Division(s)")),
                    "cofog_groups": safe_list(row.get("COFOG Group(s)")),
                    
                    "funding_sources": safe_list(row.get("Funding Sources")),
                    "sub_funding_sources": safe_list(row.get("Sub Funding Sources")),
                    "currency": currency,
                    "exchange_rate": safe_float(row.get("Exchange Rate")) or 1.0,
                    
                    "expenditure_fgn": safe_float(row.get("Expenditure – FGN")),
                    "expenditure_state": safe_float(row.get("Expenditure - State")),
                    "expenditure_ifad_loan": safe_float(row.get("Expenditure - IFAD Loan")),
                    "expenditure_ifad_grant": safe_float(row.get("Expenditure - IFAD Grant")),
                    "expenditure_oof": safe_float(row.get("Expenditure - OOF")),
                    "expenditure_beneficiary": safe_float(row.get("Expenditure - Beneficiary")),
                    "expenditure_private_sector": safe_float(row.get("Expenditure - Private Sector")),
                    "expenditure_value_chain": safe_float(row.get("Expenditure - Value Chain")),
                    "expenditure_other": safe_float(row.get("Expenditure - Other")),
                    "expenditure_total_reported": safe_float(row.get("Expenditure - Total Reported")),
                    
                    "expenditure_ifad": safe_float(row.get("Expenditure - IFAD Loan")) + safe_float(row.get("Expenditure - IFAD Grant")),
                    
                    "unit": safe_str(row.get("Beneficiary Unit")) or "Person",
                    "beneficiary_total": safe_int(row.get("Beneficiaries – Total")),
                    "beneficiary_male": safe_int(row.get("Beneficiaries - Male")),
                    "beneficiary_female": safe_int(row.get("Beneficiaries - Female")),
                    "beneficiary_youth_under35": safe_int(row.get("Beneficiaries - Youth (<35)")),
                    "beneficiary_plwd": safe_int(row.get("Beneficiaries - PLWD")),
                    "beneficiary_categories": safe_list(row.get("Beneficiary Categories")),
                    
                    "quarterly_beneficiary_data": quarterly_data,
                    
                    "value_chain_segments": safe_list(row.get("Value Chain Segment(s)")),
                    "value_chain_segments_other": safe_str(row.get("Value Chain Segments (Other)")),
                    "climate_flag": safe_str(row.get("Climate Aligned?")),
                    "data_source": safe_list(row.get("Data Source(s)")) or ["Bulk Upload"],
                    "classification_notes": safe_str(row.get("Classification Notes"))
                }
                
                if not record_data.get("lgas"):
                    errors.append({"sheet": sheet_name, "row": row_num, "error": "LGA(s) is required"})
                    continue
                if not record_data.get("threeFS_primary"):
                    errors.append({"sheet": sheet_name, "row": row_num, "error": "3FS Primary Component is required"})
                    continue

                transaction_create = TransactionCreate(**record_data)

            except Exception as e:
                import traceback
                print(f"Exception on {sheet_name} row {row_num}: {e}")
                traceback.print_exc()
                errors.append(str(e))
                break

    print(f"Total errors: {len(errors)}")

if __name__ == '__main__':
    test_upload()
