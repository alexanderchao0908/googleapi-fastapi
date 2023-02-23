from fastapi import FastAPI
from typing import Union
from pydantic import BaseModel
from pathlib import Path
import re
import gspread
from gspread import IncorrectCellLabel
from gspread.utils import CELL_ADDR_RE

MAGIC_NUMBER = 64

cred_dir = Path("/etc/secrets/")
cred_file = cred_dir / "service_account.json"
gc = gspread.service_account(filename=str(cred_file))

app = FastAPI()


@app.get("/")
async def root():
    return {"Hello World"}


class Connection(BaseModel):
    source_id: Union[str, None] = None
    source_sheet_name: Union[str, None] = None
    destination_id: Union[str, None] = None
    destination_sheet_name: Union[str, None] = None
    copy_start: Union[str, None] = None
    copy_end: Union[str, None] = None
    paste_start: Union[str, None] = None
    paste_end: Union[str, None] = None


@app.post("/sheets/import-data")
async def import_data(conn: Connection):
    try:
        check_cell_name(conn.copy_start)
        check_cell_name(conn.copy_end)
        check_cell_name(conn.paste_start)
        check_cell_name(conn.paste_end)
        # openById
        source_file = gc.open_by_key(conn.source_id)
        # ss.getSheetByName()
        source_sheet = source_file.worksheet(conn.source_sheet_name)

        # get copy range
        s_row_count, s_col_count = rowcol_count(source_sheet)
        copy_start, copy_end, paste_start, paste_end = process_pos(conn.copy_start, conn.copy_end,
                                                                   conn.paste_start, conn.paste_end,
                                                                   s_row_count, s_col_count)
        copy_range = get_copy_range(copy_start, copy_end, paste_start, paste_end)

        # get source data
        source_data = source_sheet.get(copy_range)

        # Destination setup
        dest_file = gc.open_by_key(conn.destination_id)
        #
        dest_sheet = dest_file.worksheet(conn.destination_sheet_name)
        #
        paste_range = get_paste_range(paste_start, paste_end)

        dest_sheet.update(paste_range, source_data)

    except Exception as err:
        return {
            "status": 400,
            "message": err
        }

    else:
        return {
            "status": 200,
            "message": "Your sheet was uploaded successfully!"
        }


def get_copy_range(c_start: Union[str, None], c_end: Union[str, None],
                   p_start: Union[str, None], p_end: Union[str, None]):
    if p_end is not None:
        if p_start is None:
            p_start = "A1"
        c_start_row, c_start_col = a1_to_rowcol(c_start)
        c_end_row, c_end_col = a1_to_rowcol(c_end)
        p_start_row, p_start_col = a1_to_rowcol(p_start)
        p_end_row, p_end_col = a1_to_rowcol(p_end)

        if (c_end_row - c_start_row) > (p_end_row - p_start_row):
            c_end_row = c_start_row + (p_end_row - p_start_row)
        if (c_end_col - c_start_col) > (p_end_col - p_start_col):
            c_end_col = c_start_col + (p_end_col - p_start_col)

        c_end = rowcol_to_a1(c_end_row, c_end_col)

    copy_range = c_start + ":" + c_end

    return copy_range


def get_paste_range(start: Union[str, None], end: Union[str, None]):
    paste_range = start

    if end is not None:
        paste_range = paste_range + ":" + end

    return paste_range


def check_cell_name(cell_name: Union[str, None]):
    if cell_name is not None:
        cell_name = cell_name.upper()
        match = re.fullmatch("[a-zA-Z]+[1-9][0-9]*", cell_name)
        if match is None:
            raise Exception("Input valid cell name: 'A1' for instance")

    return True


def process_pos(copy_start: Union[str, None], copy_end: Union[str, None],
                paste_start: Union[str, None], paste_end: Union[str, None],
                c_row_count: int, c_col_count: int):
    if copy_start is None:
        copy_start = "A1"
    if copy_end is None:
        copy_end = rowcol_to_a1(c_row_count, c_col_count)

    match = re.search("[a-zA-Z]+", copy_start)
    start_col_letter = match.group(0)
    match = re.search("[1-9][0-9]*", copy_start)
    start_row_number = int(match.group(0))

    match = re.search("[a-zA-Z]+", copy_end)
    end_col_letter = match.group(0)
    match = re.search("[1-9][0-9]*", copy_end)
    end_row_number = int(match.group(0))

    if start_row_number > end_row_number:
        raise Exception("Start row number should be not be larger than end row number in Source Sheet.")
    elif cmp_str(end_col_letter, start_col_letter) == 1:
        raise Exception("Start column letter should not be after end column letter in Source Sheet.")

    if paste_start is None:
        paste_start = "A1"

    if paste_end is not None:
        match = re.search("[a-zA-Z]+", paste_start)
        start_col_letter = match.group(0)
        match = re.search("[1-9][0-9]*", paste_start)
        start_row_number = int(match.group(0))

        match = re.search("[a-zA-Z]+", paste_end)
        end_col_letter = match.group(0)
        match = re.search("[1-9][0-9]*", paste_end)
        end_row_number = int(match.group(0))

        if start_row_number > end_row_number:
            raise Exception("Start row number should not be larger than end row number in destination Sheet.")
        elif cmp_str(end_col_letter, start_col_letter) == 1:
            raise Exception("Start column letter should not be after end column letter in destination Sheet.")

    return copy_start, copy_end, paste_start, paste_end


def rowcol_count(wks):
    max_cols = 0
    for non_empty_row_num in range(1, wks.row_count):  # step through non-empty rows
        cols_in_row = len(wks.row_values(non_empty_row_num))  # number of cols in this non-empty row
        if cols_in_row > max_cols:
            max_cols = cols_in_row
        if cols_in_row == 0:  # only process if not empty
            break  # stop getting new rows at first empty row
    max_rows = len(wks.get_all_values())  # just the non-empty row count

    return max_rows, max_cols


def rowcol_to_a1(row: int, col: int):
    """Translates a row and column cell address to A1 notation.
    :param row: The row of the cell to be converted.
        Rows start at index 1.
    :type row: int, str
    :param col: The column of the cell to be converted.
        Columns start at index 1.
    :type row: int, str
    :returns: a string containing the cell's coordinates in A1 notation.
    Example:
    # >>> rowcol_to_a1(1, 1)
    A1
    """
    row = int(row)
    col = int(col)

    if row < 1 or col < 1:
        raise IncorrectCellLabel("({}, {})".format(row, col))

    div = col
    column_label = ""

    while div:
        (div, mod) = divmod(div, 26)
        if mod == 0:
            mod = 26
            div -= 1
        column_label = chr(mod + MAGIC_NUMBER) + column_label

    label = "{}{}".format(column_label, row)

    return label


def a1_to_rowcol(label: str):
    """Translates a cell's address in A1 notation to a tuple of integers.
    :param str label: A cell label in A1 notation, e.g. 'B1'.
        Letter case is ignored.
    :returns: a tuple containing `row` and `column` numbers. Both indexed
              from 1 (one).
    :rtype: tuple
    Example:
    # >>> a1_to_rowcol('A1')
    (1, 1)
    """
    m = CELL_ADDR_RE.match(label)

    if m:
        column_label = m.group(1).upper()
        row = int(m.group(2))

        col = 0
        for i, c in enumerate(reversed(column_label)):
            col += (ord(c) - MAGIC_NUMBER) * (26 ** i)
    else:
        raise IncorrectCellLabel(label)

    return row, col


def cmp_str(a: str, b: str):
    res = 1
    if len(a) < len(b):
        res = 1
    if len(a) > len(b):
        res = -1
    if len(a) == len(b):
        if a < b:
            res = 1
        elif a > b:
            res = -1
        else:
            res = 0

    return res
