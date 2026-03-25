from flask import Flask, request, jsonify
import requests
import io
import zipfile
import re
import traceback

app = Flask(__name__)

def load_workbook_safe(content):
    zin = zipfile.ZipFile(io.BytesIO(content))
    zout_buffer = io.BytesIO()
    zout = zipfile.ZipFile(zout_buffer, 'w', zipfile.ZIP_DEFLATED)
    for item in zin.infolist():
        data = zin.read(item.filename)
        if item.filename.startswith('xl/worksheets/') and item.filename.endswith('.xml'):
            try:
                xml = data.decode('utf-8')
                xml = re.sub(r'<dataValidations[^>]*>.*?</dataValidations>', '', xml, flags=re.DOTALL)
                xml = re.sub(r'<dataValidation[^/]*/>', '', xml)
                data = xml.encode('utf-8')
            except Exception:
                pass
        zout.writestr(item, data)
    zout.close()
    zout_buffer.seek(0)
    return zout_buffer.read()

@app.route('/update-excel', methods=['POST'])
def update_excel():
    try:
        data = request.json
        webhook = data.get('webhook')
        file_id = data.get('file_id')
        updates = data.get('updates')
        r = requests.get(f'{webhook}/disk.file.get.json?id={file_id}')
        file_info = r.json()
        download_url = file_info['result']['DOWNLOAD_URL']
        r = requests.get(download_url)
        clean_content = load_workbook_safe(r.content)
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(clean_content))
        ws = wb.active
        header_row = None
        sum_col = None
        tt_col = None
        for row in ws.iter_rows():
            for cell in row:
                if cell.value == 'Сумма заявки':
                    sum_col = cell.column
                    header_row = cell.row
                if cell.value == 'Код ТТ':
                    tt_col = cell.column
            if header_row:
                break
        if not header_row or not sum_col or not tt_col:
            return jsonify({'status': 'error', 'message': 'Колонки не найдены'}), 400
        updated = 0
        for update in updates:
            tt_code = update.get('tt_code')
            fact = update.get('fact')
            for row in ws.iter_rows(min_row=header_row + 1):
                if str(row[tt_col - 1].value).strip() == str(tt_code).strip():
                    row[sum_col - 1].value = fact
                    updated += 1
                    break
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        upload_url_r = requests.get(f'{webhook}/disk.file.uploadversion.json?id={file_id}')
        upload_debug = upload_url_r.json()
        return jsonify({'status': 'ok', 'updated': updated, 'debug': upload_debug})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e), 'trace': traceback.format_exc()}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
