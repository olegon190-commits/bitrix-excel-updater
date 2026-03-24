from flask import Flask, request, jsonify
import openpyxl
import requests
import io

app = Flask(__name__)

@app.route('/update-excel', methods=['POST'])
def update_excel():
    data = request.json
    webhook = data.get('webhook')
    file_id = data.get('file_id')
    updates = data.get('updates')
    
    # Получаем DOWNLOAD_URL
    r = requests.get(f'{webhook}/disk.file.get.json?id={file_id}')
    file_info = r.json()
    download_url = file_info['result']['DOWNLOAD_URL']
    
    # Скачиваем файл
    r = requests.get(download_url)
    wb = openpyxl.load_workbook(io.BytesIO(r.content))
    ws = wb.active
    
    # Находим колонку "Сумма заявки"
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
    
    # Обновляем данные
    for update in updates:
        tt_code = update['tt_code']
        fact = update['fact']
        for row in ws.iter_rows(min_row=header_row+1):
            if row[tt_col-1].value == tt_code:
                row[sum_col-1].value = fact
                break
    
    # Сохраняем в память
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    
    # Загружаем обратно на Bitrix
    upload_r = requests.post(
        f'{webhook}/disk.file.uploadversion.json?id={file_id}',
        files={'file': ('file.xlsx', output, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
    )
    
    return jsonify({'status': 'ok', 'result': upload_r.json()})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
