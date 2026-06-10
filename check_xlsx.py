import openpyxl

def check_validations(filename):
    wb = openpyxl.load_workbook(filename)
    for sheet in wb.sheetnames:
        if sheet == "Options": continue
        ws = wb[sheet]
        print(f"Sheet: {sheet}")
        print(f"  Validations count: {len(ws.data_validations.dataValidation)}")
        for dv in ws.data_validations.dataValidation:
            print(f"    - Type: {dv.type}, Formula: {dv.formula1}, Cells: {dv.cells}")

if __name__ == "__main__":
    check_validations("test_template.xlsx")
