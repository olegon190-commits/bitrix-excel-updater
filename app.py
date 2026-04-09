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

RESHETOVA_CODES = set(['T1926', 'T0863', 'T0845', 'T0878', 'T0836', 'T0979', 'T1760', 'T1791', 'T6172', 'T1966', 'T3880', 'T1003', 'T3705', 'T0905', 'T1741', 'T8613', 'T0823', 'T7548', 'T0948', 'T8370', 'T5322', 'T1723', 'T8788', 'T6552', 'T0876', 'T1086', 'T2324', 'T7369', 'T1173', 'T3420', 'T6781', 'T0893', 'T0895', 'T3897', 'T5541', 'T4009', 'T1734', 'T6297', 'T5889', 'T4175', 'T6302', 'T8278', 'T6859', 'T6732', 'T6733', 'T6339', 'T6040', 'T6004', 'T8546', 'T3661', 'T6871', 'T6421', 'T3182', 'T7355', 'T5999', 'T1981', 'T5875', 'T5509', 'T8485', 'T7406', 'T3677', 'T3870', 'T4053', 'T3532', 'T5595', 'T3550', 'T3321', 'T3529', 'T1869', 'T1874', 'T3535', 'T5170', 'T0504', 'T8670', 'T1721', 'T6405', 'T3541', 'T6353', 'T0911', 'T7694', 'T1153', 'T1758', 'T5470', 'T3309', 'T0980', 'T7555', 'T4030', 'T4029', 'T0977', 'T0832', 'T6354', 'T0799', 'T7257', 'T7303', 'T6292', 'T5594', 'T3770', 'T3308', 'T3534', 'T5328', 'T5650', 'T3181', 'T3316', 'T8615', 'T5349', 'T8726', 'T5667', 'T0925', 'T5656', 'T1037', 'T5255', 'T6598', 'T6729', 'T0985', 'T3615', 'T3614', 'T3652', 'T2011', 'T5763', 'T3163', 'T6101', 'T1026', 'T1937', 'T3608', 'T6105', 'T8416', 'T6490', 'T1135', 'T3800', 'T1743', 'T1820', 'T1959', 'T8355', 'T0912', 'T3885', 'T1004', 'T1005', 'T5302', 'T4075', 'T6330', 'T7669', 'T4061', 'T3773', 'T3570', 'T3551', 'T3687', 'T8372', 'T5792', 'T3716', 'T3693', 'T3500', 'T3543', 'T2610', 'T2607', 'T3573', 'T6205', 'T6240', 'T5992', 'T0835', 'T2003', 'T8375', 'T5921', 'T1744', 'T6045', 'T3981', 'T0941', 'T1181', 'T1795', 'T1812', 'T1829', 'T3961', 'T7502', 'T7391', 'T3975', 'T5197', 'T1700', 'T8778', 'T7417', 'T5184', 'T5900', 'T8400', 'T8362', 'T5887', 'T7682', 'T6689', 'T5280', 'T5199', 'T7597', 'T5890', 'T6175', 'T6174', 'T7620', 'T0849', 'T5619', 'T3542', 'T1901', 'T3741', 'T5941', 'T6334', 'T7064', 'T5864', 'T0899', 'T5862', 'T0215', 'T6664', 'T3458', 'T1277', 'T6867', 'T5153', 'T7389', 'T1853', 'T6453', 'T7547', 'T1847', 'T5588', 'T1799', 'T7613', 'T1809', 'T7276', 'T6941', 'T6173', 'T1885', 'T7671', 'T2328', 'T8321', 'T5948', 'T4179', 'T3667', 'T8386', 'T8389', 'T6752', 'T1964', 'T5689', 'T3884', 'T5385', 'T3972', 'T6290', 'T5432', 'T7561', 'T0389', 'T6845', 'T7132', 'T6677', 'T4188', 'T4163', 'T1977', 'T6383', 'T2005', 'T3545', 'T0989', 'T3431', 'T1726', 'T3817', 'T5547', 'T0901', 'T8771', 'T0942', 'T3621', 'T3576', 'T3577', 'T7315', 'T3538', 'T5077', 'T6332', 'T8323'])

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
        wb_readonly = openpyxl.load_workbook(io.BytesIO(clean_content), data_only=True)

        today_sheet = get_yesterday_sheet_name()
        if today_sheet not in wb.sheetnames:
            return jsonify({'status': 'error', 'message': f'Вкладка {today_sheet} не найдена', 'sheets': wb.sheetnames}), 400

        ws = wb[today_sheet]
        tt_col, sum_col, plan_col, dev_col, otklonenie_col, header_row = find_columns(ws)

        if not header_row or not sum_col or not tt_col:
            return jsonify({'status': 'error', 'message': 'Колонки не найдены'}), 400

        updates_map = {str(u.get('tt_code')).strip(): u.get('fact') for u in updates}

        # Шаг 1 — записываем факт и собираем найденные ТТ
        updated = 0
        found_codes = set()
        for row in ws.iter_rows(min_row=header_row + 1):
            tt = str(row[tt_col - 1].value or '').strip().replace('\xa0', '').replace(' ', '')
            if not tt:
                continue
            found_codes.add(tt)
            if tt in updates_map:
                row[sum_col - 1].value = round(float(updates_map[tt]), 2)
                updated += 1

        # Шаг 2 — считаем накопленное отклонение через wb_readonly
        prev_sheets = get_previous_sheets(wb, today_sheet)
        accumulated = {}

        for sheet_name in prev_sheets:
            ws_prev = wb_readonly[sheet_name]
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

        # Шаг 4 — добавляем внеплановые ТТ
        unplanned_added = 0
        debug_not_found = []
        for tt_code, fact in updates_map.items():
            if tt_code not in found_codes:
                debug_not_found.append(tt_code)
                if tt_code in RESHETOVA_CODES and fact and float(fact) != 0:
                    new_row = [None] * ws.max_column
                    new_row[tt_col - 1] = tt_code
                    new_row[sum_col - 1] = round(float(fact), 2)
                    ws.append(new_row)
                    unplanned_added += 1

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
            'unplanned_added': unplanned_added,
            'debug_not_found': debug_not_found,
            'prev_sheets_processed': len(prev_sheets),
            'result': upload_r.json()
        })

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e), 'trace': traceback.format_exc()}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
