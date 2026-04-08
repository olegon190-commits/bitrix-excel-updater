from flask import Flask, request, jsonify
import requests
import io
import zipfile
import re
import traceback
import base64
from datetime import datetime, timedelta, timezone

app = Flask(__name__)

DAYS_RU = {0: 'пн', 1: 'вт', 2: 'ср', 3: 'чт', 4: 'пт', 5: 'сб', 6: 'вс'}

def get_yesterday_sheet_name():
    MSK = timezone(timedelta(hours=3))
    d = datetime.now(MSK) - timedelta(days=1)
    while d.weekday() >= 5:
        d = d - timedelta(days=1)
    day = d.day
    weekday = DAYS_RU[d.weekday()]
    return f"{day:02d} {weekday}"

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

def get_sheet_day(sheet_name):
    try:
        return int(sheet_name.split()[0])
    except:
        return None

def get_previous_sheets(wb, today_sheet):
    today_day = get_sheet_day(today_sheet)
    if today_day is None:
        return []
    prev = []
    for name in wb.sheetnames:
        if name == 'Контроль':
            continue
        day = get_sheet_day(name)
        if day is not None and day < today_day:
            prev.append(name)
    return prev

def find_columns(ws):
    tt_col = sum_col = plan_col = dev_col = otklonenie_col = header_row = None
    for row in ws.iter_rows(max_row=5):
        for cell in row:
            v = cell.value
            if v == 'Код ТТ': tt_col = cell.column
            if v == 'Сумма заявки': sum_col = cell.column; header_row = cell.row
            if v == 'План сумма': plan_col = cell.column
            if v == 'Отклонения дня': dev_col = cell.column
            if v == 'Отклонение': otklonenie_col = cell.column
        if header_row:
            break
    return tt_col, sum_col, plan_col, dev_col, otklonenie_col, header_row

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

        today_sheet = get_yesterday_sheet_name()
        if today_sheet not in wb.sheetnames:
            return jsonify({'status': 'error', 'message': f'Вкладка {today_sheet} не найдена', 'sheets': wb.sheetnames}), 400

        ws = wb[today_sheet]
        tt_col, sum_col, plan_col, dev_col, otklonenie_col, header_row = find_columns(ws)

        if not header_row or not sum_col or not tt_col:
            return jsonify({'status': 'error', 'message': 'Колонки не найдены'}), 400

        updates_map = {str(u.get('tt_code')).strip(): u.get('fact') for u in updates}

        # Шаг 1 — записываем факт
        updated = 0
        for row in ws.iter_rows(min_row=header_row + 1):
            tt = str(row[tt_col - 1].value or '').strip().replace('\xa0', '').replace(' ', '')
            if not tt:
                continue
            if tt in updates_map:
                row[sum_col - 1].value = round(float(updates_map[tt]), 2)
                updated += 1

        # Шаг 2 — считаем накопленное отклонение по колонке "Отклонение"
        prev_sheets = get_previous_sheets(wb, today_sheet)
        accumulated = {}

        for sheet_name in prev_sheets:
            ws_prev = wb[sheet_name]
            tt_col_p, sum_col_p, plan_col_p, _, otklonenie_col_p, header_row_p = find_columns(ws_prev)
            if not header_row_p or not tt_col_p:
                continue
            if not otklonenie_col_p:
                continue
            for row in ws_prev.iter_rows(min_row=header_row_p + 1):
                tt = str(row[tt_col_p - 1].value or '').strip().replace('\xa0', '').replace(' ', '')
                if not tt:
                    continue
                otklonenie = row[otklonenie_col_p - 1].value
                # Пропускаем если это формула или пусто
                if otklonenie is None or isinstance(otklonenie, str):
                    continue
                otklonenie = float(otklonenie)
                accumulated[tt] = accumulated.get(tt, 0) + otklonenie

        # Шаг 3 — записываем отклонения дня
        dev_updated = 0
        if dev_col:
            for row in ws.iter_rows(min_row=header_row + 1):
                tt = str(row[tt_col - 1].value or '').strip().replace('\xa0', '').replace(' ', '')
                if not tt:
                    continue
                if tt in accumulated and accumulated[tt] != 0:
                    row[dev_col - 1].value = round(accumulated[tt], 2)
                    dev_updated += 1

        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        file_content_b64 = base64.b64encode(output.read()).decode('utf-8')

        upload_r = requests.post(
            f'{webhook}/disk.file.uploadversion.json',
            json={'id': file_id, 'fileContent': ['file.xlsx', file_content_b64]}
        )

        return jsonify({
            'status': 'ok',
            'sheet': today_sheet,
            'updated_fact': updated,
            'updated_deviation': dev_updated,
            'prev_sheets_processed': len(prev_sheets),
            'result': upload_r.json()
        })

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e), 'trace': traceback.format_exc()}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
