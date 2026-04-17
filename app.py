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

RESHETOVA_CODES = set([
    'T0215', 'T0389', 'T0504', 'T0788', 'T0799', 'T0816', 'T0823', 'T0832', 'T0835', 'T0836',
    'T0845', 'T0849', 'T0854', 'T0863', 'T0867', 'T0876', 'T0878', 'T0879', 'T0893', 'T0895',
    'T0899', 'T0901', 'T0905', 'T0911', 'T0912', 'T0925', 'T0931', 'T0941', 'T0942', 'T0943',
    'T0948', 'T0977', 'T0979', 'T0980', 'T0985', 'T0989', 'T1003', 'T1004', 'T1005', 'T1026',
    'T1037', 'T1041', 'T1064', 'T1086', 'T1135', 'T1153', 'T1173', 'T1181', 'T1250', 'T1251',
    'T1277', 'T1338', 'T1650', 'T1700', 'T1717', 'T1721', 'T1723', 'T1726', 'T1734', 'T1738',
    'T1741', 'T1743', 'T1744', 'T1748', 'T1758', 'T1760', 'T1772', 'T1791', 'T1795', 'T1797',
    'T1798', 'T1799', 'T1802', 'T1809', 'T1812', 'T1816', 'T1820', 'T1821', 'T1822', 'T1829',
    'T1832', 'T1847', 'T1849', 'T1853', 'T1857', 'T1869', 'T1874', 'T1883', 'T1885', 'T1901',
    'T1904', 'T1909', 'T1926', 'T1937', 'T1957', 'T1959', 'T1964', 'T1966', 'T1967', 'T1970',
    'T1977', 'T1981', 'T1993', 'T2003', 'T2005', 'T2011', 'T2013', 'T2033', 'T2273', 'T2300',
    'T2324', 'T2328', 'T2341', 'T2607', 'T2610', 'T3139', 'T3163', 'T3181', 'T3182', 'T3308',
    'T3309', 'T3316', 'T3321', 'T3420', 'T3431', 'T3434', 'T3458', 'T3490', 'T3496', 'T3500',
    'T3529', 'T3531', 'T3532', 'T3534', 'T3535', 'T3538', 'T3541', 'T3542', 'T3543', 'T3545',
    'T3546', 'T3547', 'T3550', 'T3551', 'T3559', 'T3570', 'T3573', 'T3576', 'T3577', 'T3578',
    'T3593', 'T3608', 'T3611', 'T3612', 'T3614', 'T3615', 'T3621', 'T3641', 'T3643', 'T3646',
    'T3647', 'T3648', 'T3652', 'T3661', 'T3667', 'T3677', 'T3687', 'T3693', 'T3694', 'T3704',
    'T3705', 'T3716', 'T3717', 'T3741', 'T3760', 'T3766', 'T3770', 'T3773', 'T3800', 'T3817',
    'T3867', 'T3870', 'T3878', 'T3880', 'T3882', 'T3884', 'T3885', 'T3896', 'T3897', 'T3961',
    'T3972', 'T3975', 'T3976', 'T3981', 'T4009', 'T4012', 'T4029', 'T4030', 'T4053', 'T4061',
    'T4075', 'T4130', 'T4135', 'T4148', 'T4163', 'T4175', 'T4179', 'T4188', 'T5077', 'T5137',
    'T5153', 'T5161', 'T5170', 'T5175', 'T5184', 'T5185', 'T5194', 'T5197', 'T5199', 'T5209',
    'T5255', 'T5256', 'T5260', 'T5280', 'T5302', 'T5322', 'T5328', 'T5349', 'T5385', 'T5432',
    'T5457', 'T5470', 'T5478', 'T5509', 'T5541', 'T5547', 'T5588', 'T5594', 'T5595', 'T5596',
    'T5597', 'T5598', 'T5599', 'T5600', 'T5602', 'T5614', 'T5619', 'T5650', 'T5652', 'T5656',
    'T5667', 'T5669', 'T5689', 'T5763', 'T5792', 'T5813', 'T5848', 'T5858', 'T5862', 'T5864',
    'T5866', 'T5875', 'T5879', 'T5887', 'T5889', 'T5890', 'T5900', 'T5921', 'T5929', 'T5935',
    'T5940', 'T5941', 'T5948', 'T5967', 'T5981', 'T5992', 'T5999', 'T6000', 'T6004', 'T6023',
    'T6038', 'T6040', 'T6045', 'T6061', 'T6066', 'T6074', 'T6081', 'T6082', 'T6089', 'T6101',
    'T6105', 'T6146', 'T6158', 'T6171', 'T6172', 'T6173', 'T6174', 'T6175', 'T6188', 'T6197',
    'T6205', 'T6207', 'T6221', 'T6239', 'T6240', 'T6268', 'T6283', 'T6289', 'T6290', 'T6291',
    'T6292', 'T6297', 'T6302', 'T6311', 'T6313', 'T6322', 'T6327', 'T6330', 'T6332', 'T6334',
    'T6339', 'T6349', 'T6353', 'T6354', 'T6356', 'T6379', 'T6383', 'T6399', 'T6405', 'T6409',
    'T6412', 'T6421', 'T6422', 'T6441', 'T6445', 'T6453', 'T6467', 'T6474', 'T6479', 'T6484',
    'T6490', 'T6495', 'T6508', 'T6512', 'T6532', 'T6552', 'T6572', 'T6581', 'T6588', 'T6598',
    'T6630', 'T6632', 'T6646', 'T6658', 'T6664', 'T6665', 'T6666', 'T6677', 'T6680', 'T6689',
    'T6692', 'T6698', 'T6711', 'T6716', 'T6719', 'T6723', 'T6728', 'T6729', 'T6732', 'T6733',
    'T6742', 'T6743', 'T6752', 'T6759', 'T6781', 'T6788', 'T6810', 'T6811', 'T6816', 'T6843',
    'T6845', 'T6852', 'T6857', 'T6859', 'T6867', 'T6871', 'T6883', 'T6893', 'T6894', 'T6906',
    'T6907', 'T6914', 'T6918', 'T6922', 'T6939', 'T6941', 'T6942', 'T6945', 'T6948', 'T6951',
    'T6955', 'T6956', 'T6963', 'T6964', 'T6966', 'T6984', 'T6985', 'T6991', 'T7021', 'T7026',
    'T7027', 'T7033', 'T7035', 'T7051', 'T7064', 'T7065', 'T7066', 'T7096', 'T7099', 'T7109',
    'T7112', 'T7119', 'T7120', 'T7130', 'T7132', 'T7135', 'T7141', 'T7158', 'T7176', 'T7179',
    'T7181', 'T7187', 'T7226', 'T7243', 'T7254', 'T7257', 'T7276', 'T7278', 'T7298', 'T7303',
    'T7311', 'T7315', 'T7338', 'T7342', 'T7343', 'T7351', 'T7355', 'T7361', 'T7362', 'T7363',
    'T7364', 'T7365', 'T7369', 'T7385', 'T7389', 'T7391', 'T7406', 'T7417', 'T7425', 'T7427',
    'T7428', 'T7429', 'T7430', 'T7443', 'T7445', 'T7451', 'T7458', 'T7459', 'T7463', 'T7476',
    'T7478', 'T7480', 'T7501', 'T7502', 'T7505', 'T7527', 'T7545', 'T7546', 'T7547', 'T7548',
    'T7555', 'T7557', 'T7561', 'T7589', 'T7597', 'T7602', 'T7604', 'T7609', 'T7613', 'T7620',
    'T7634', 'T7669', 'T7671', 'T7682', 'T7687', 'T7694', 'T7704', 'T8278', 'T8309', 'T8321',
    'T8323', 'T8324', 'T8355', 'T8362', 'T8368', 'T8370', 'T8372', 'T8375', 'T8378', 'T8379',
    'T8382', 'T8383', 'T8386', 'T8389', 'T8395', 'T8400', 'T8402', 'T8414', 'T8416', 'T8419',
    'T8422', 'T8424', 'T8427', 'T8430', 'T8437', 'T8444', 'T8452', 'T8468', 'T8479', 'T8482',
    'T8484', 'T8485', 'T8527', 'T8532', 'T8546', 'T8547', 'T8558', 'T8575', 'T8585', 'T8593',
    'T8597', 'T8613', 'T8615', 'T8624', 'T8633', 'T8646', 'T8648', 'T8650', 'T8669', 'T8670',
    'T8680', 'T8688', 'T8726', 'T8754', 'T8760', 'T8771', 'T8778', 'T8785', 'T8788',
])
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

def find_itogo_row(ws):
    for row in ws.iter_rows():
        for cell in row:
            if cell.value and str(cell.value).strip() == 'Итого' and cell.column == 1:
                return cell.row
    return None

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

        # Находим блок итогов — 4 строки менеджеров + Итого
        itogo_row = find_itogo_row(ws)
        first_summary_row = itogo_row - 4 if itogo_row else None

        # Находим последнюю плановую ТТ (до блока итогов)
        last_tt_row = header_row
        if first_summary_row:
            for row in ws.iter_rows(min_row=header_row + 1, max_row=first_summary_row - 1):
                tt = str(row[tt_col - 1].value or '').strip()
                if tt and tt.startswith('T') and len(tt) == 5:
                    last_tt_row = row[0].row

        # Список внеплановых для добавления
        unplanned_to_add = []
        for tt_code, fact in updates_map.items():
            if tt_code not in found_codes:
                debug_not_found.append(tt_code)
                if tt_code in RESHETOVA_CODES and fact and float(fact) != 0:
                    unplanned_to_add.append((tt_code, fact))

        # Если не хватает пустых строк — вставляем перед блоком итогов
        if first_summary_row:
            free_rows = first_summary_row - last_tt_row - 1
            if len(unplanned_to_add) > free_rows:
                needed = len(unplanned_to_add) - free_rows
                ws.insert_rows(first_summary_row, amount=needed)

        # Записываем внеплановые
        current_row = last_tt_row + 1
        for tt_code, fact in unplanned_to_add:
            ws.cell(row=current_row, column=tt_col).value = tt_code
            ws.cell(row=current_row, column=sum_col).value = round(float(fact), 2)
            current_row += 1
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
