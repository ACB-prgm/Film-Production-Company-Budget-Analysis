# %%
# https://www.dropbox.com/developers/apps/info/pu644k33a199294#settings

from concurrent.futures import ThreadPoolExecutor, wait
import pandas as pd
import numpy as np
import dropbox
import camelot
import tempfile
import fitz
import os
import re

# %%
def contains(string:str, contains:list) -> bool:
    for cont in contains:
        if cont in string:
            return True
    
    return False

# %% [markdown]
# ## Classification

# %%
def get_content(extension, file_obj):
    if extension == ".pdf":
        reader = fitz.open(stream=file_obj)
        return reader.load_page(0).get_text()
    elif extension == ".xlsx":
        return pd.read_excel(file_obj).to_string()
    elif extension == ".xlsb":
        return pd.read_excel(file_obj, engine='pyxlsb').to_string()
    else:
        return None


def classify_file(path, file_obj, verbose=False):
    try:
        file_name = path.split("/")[-1].lower()
        if contains(file_name, ["po log", "purchase order"]):
            return "PO"

        extension = os.path.splitext(path)[1]
        content = get_content(extension, file_obj)

        content = content.lower()
        if "purchase order" in content:
            return "PO"
        elif contains(content, ["cost summary", "hot budget", "film production cost summary"]):
            return "CS"
        elif "wrapbook" in content:
            return "OTHER"
        elif "payroll" in content:
            return "PR"
        else:
            return "OTHER"
    except Exception as e:
        print("classification error %s at: " % e, path) if verbose else None
        return "OTHER"

# %% [markdown]
# ## Department Getter

# %%
def get_dept_from_line(ln:int) -> str:
    try:
        ln = int(ln)
    except ValueError:
        return ln

    if ln in range(51):
        return "PRE-PRODUCTION | WRAP LABOR"
    elif ln in range(51, 101):
        return "SHOOTING LABOR"
    elif ln in range(101, 114):
        return "PRE-PRODUCTION | WRAP EXPENSES"
    elif ln in range(114, 140):
        return "LOCATION AND TRAVEL"
    elif ln in range(140,151):
        return "MAKEUP, WARDROBE, AND ANIMALS"
    elif ln in range(151, 168):
        return "STUDIO | STAGE RENTAL / EXPENSES"
    elif ln in range(168,181):
        return "ART DEPARTMENT LABOR"
    elif ln in range(181, 193):
        return "ART DEPARTMENT EXPENSES"
    elif ln in range(193, 211):
        return "EQUIPMENT COSTS"
    elif ln in range(211, 217):
        return "FILMSTOCK, DEVELOP AND PRINT"
    elif ln in range(217,227):
        return "MISCELLANEOUS"
    elif ln in range(227, 234):
        return "DIRECTOR | CREATIVE FEES"
    elif ln in range(234, 271):
        return "TALENT LABOR"
    elif ln in range(271, 277):
        return "TALENT EXPENSES"
    elif ln in range(277, 282):
        return "POST PRODUCTION LABOR"
    elif ln in range(282, 330):
        return "EDITORIAL | FINISHING | POST PRODUCTION"
    else:
        return "OTHER"

# %% [markdown]
# ## Helpers

# %%
def camelot_read_pdf_bytes(file_obj, table_num=0) -> pd.DataFrame:
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_pdf:
        temp_pdf.write(file_obj)
        return camelot.read_pdf(temp_pdf.name)._tables[table_num].df.copy()

# %%
def get_start(_df:pd.DataFrame, key:str) -> int:
    try:
        return (_df == key).any(axis=1).idxmax()
    except ValueError:
        return 0

# %%
def read_sheet(file_obj, extension:str) -> pd.DataFrame:
    if extension == ".xlsx":
        _df = pd.read_excel(file_obj)
    elif extension == ".xlsb":
        _df = pd.read_excel(file_obj, engine='pyxlsb')
    
    start = get_start(_df, "LINE")
    if not "ACTUAL" in _df.iloc[start]:
        _df.columns = _df.iloc[start].fillna(_df.iloc[start-1])
        end = _df[start:].isna().all(axis=1).idxmax()
        _df = _df.iloc[start+1 : end]
    else:
        _df.columns = _df.iloc[start]
        _df = _df.iloc[start+1]

    _df.dropna(subset=["LINE", "PAYEE"], inplace=True)
    _df = _df.replace(["\)", ","], "", regex=True).replace("\(", "-", regex=True)
    _df.ACTUAL = pd.to_numeric(_df.ACTUAL, errors="coerce").astype(float)
    if "RATE" in _df.columns:
        _df.RATE = _df.RATE.astype(float)
    
    return _df

# %% [markdown]
# ## Cost Summary Reader

# %%
HB_CS_COLS = ["SECTION", "drop", "BID TOTALS", "ACTUAL", "VARIANCE"]


def read_hot_budget_cs(file_obj, extension) -> pd.DataFrame:
    if extension == ".pdf":
        _df = camelot_read_pdf_bytes(file_obj, 1)
        
        _df.drop(12, inplace=True)

        _df.columns = HB_CS_COLS
        _df.drop(columns=["drop"], inplace=True)
        _df = _df.loc[1:]

        _df = _df.replace([r"CS\d+\b ", r".*\n", "\)", ","], "", regex=True).replace("\(", "-", regex=True)

        _df[_df.columns[1:]] = _df.iloc[:, 1:].replace("", np.nan).astype(float)

        _df = _df.dropna(thresh=2)

        return _df.reset_index(drop=True)
    elif extension == ".xlsx":
        _df = pd.read_excel(file_obj)
        start = get_start(_df, "ESTIMATED COST SUMMARY")
        _df.columns = _df.iloc[start]
        _df = _df.iloc[start+1: start + 24]

        dir_cost = get_start(_df, "Direct Costs A - K")
        if dir_cost:
            _df.drop(dir_cost, inplace=True)
        
        _df = _df.dropna(how="all", axis=1).drop(11).dropna(thresh=3).rename(columns={"ESTIMATED COST SUMMARY":"SECTION"})
        _df.drop(_df.columns[1], axis=1, inplace=True)

        sep_nums = lambda x: x[re.search(r"\d ", x).end():]
        _df.SECTION = _df.SECTION.apply(sep_nums)

        return _df.reset_index(drop=True)
    else:
        return pd.DataFrame()


def read_GetActual_cs(file_obj) -> pd.DataFrame:
    reader = fitz.open(stream=file_obj)
    content = reader.load_page(0).get_text()

    start = re.search(r"\b[A-Z]\s", content[2:]).start()
    content = re.sub(r"\b[A-Z]\s|Bid Actual|\,|\)", "", content.replace("(", "-"))
    content = content[start:content.find("\nGRAND TOTAL")].split("\n")
    _df = pd.DataFrame(columns=["SECTION", "BID TOTALS", "ACTUAL"])
    
    for line in content:
        vals = line.split("$")
        if len(vals) > 1:
            _df.loc[len(_df)] = vals[:3]

    _df[["BID TOTALS", "ACTUAL"]] = _df[["BID TOTALS", "ACTUAL"]].astype(float)
    _df = _df.drop(_df[_df.SECTION.str.contains("SUB TOTAL")].index)

    _df["VARIANCE"] = _df["ACTUAL"] - _df["BID TOTALS"]
    _df.SECTION = _df.SECTION.apply(str.strip)

    return _df


def read_cost_summary(file_obj, extension) -> pd.DataFrame:
    content = get_content(extension, file_obj)
    
    if "ESTIMATED COST SUMMARY" in content:
        _df = read_hot_budget_cs(file_obj, extension)
    elif "Film Production Cost Summary" in content:
        _df = read_GetActual_cs(file_obj)
    else:
        return pd.DataFrame()
    
    _df.fillna(0, inplace=True)

    return _df

# %% [markdown]
# ## Payroll Reader

# %%
PR_COLS = ['LINE', 'PAYEE', 'PO', 'F1', 'F2', 'DAYS', 'RATE', 'BASE', '1.5', '2', '3', 'TAXABLE', 'NON-TAX', 'TOTAL ST', 'TOTAL OT', 'ACTUAL', 'FRINGE 1', 'FRINGE 2', 'LINE DESCRIPTION']

def read_pdf_payroll(file_obj) -> pd.DataFrame:
    _df = camelot_read_pdf_bytes(file_obj, 0)
    
    _df.columns = PR_COLS
    _df = _df.iloc[1:].reset_index(drop=True).replace("", np.nan).dropna(how="all")

    _df.LINE.fillna(_df.PAYEE, inplace=True)
    _df[['LINE', 'PAYEE']] = _df.LINE.str.split(" ", n=1, expand=True)

    _df = _df.replace(["\)", ","], "", regex=True).replace("\(", "-", regex=True)
    _df.ACTUAL = _df.ACTUAL.astype(float)

    return _df

# %%
def read_payroll(file_obj, extension) -> pd.DataFrame:
    if extension == ".pdf":
        _df = read_pdf_payroll(file_obj)
    elif extension in [".xlsx", "xlsb"]:
        _df = read_sheet(file_obj, extension)
    else:
        return pd.DataFrame()
    
    _df = _df.dropna(subset="PAYEE")
    _df.RATE = _df.RATE.astype(float)
    _df.DAYS = _df.DAYS.astype(float)

    _df["EST"] = _df.RATE * _df.DAYS
    _df["VARIANCE"] = _df.ACTUAL - _df.EST
    _df["VAR_PCT"] = _df.VARIANCE / _df.EST * 100
    _df["SECTION"] = _df.LINE.apply(get_dept_from_line)

    return _df[["LINE", "SECTION", "PAYEE", "RATE", "EST", "ACTUAL", "VARIANCE", "VAR_PCT", "LINE DESCRIPTION"]]

# %% [markdown]
# ## Purchase Order Log Reader

# %%
PO_COLS = ["LINE", "PAYEE", "PO", "DATE", "PAYID", "ACTUAL", "LINE DESCRIPTION"]


def read_pdf_purchase_order(file_obj) -> pd.DataFrame:
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_pdf:
        temp_pdf.write(file_obj)
        _df = camelot.read_pdf(temp_pdf.name)._tables[0].df.copy()
    
    _df.columns = PO_COLS
    _df = _df.iloc[1:].reset_index(drop=True).replace("", np.nan).dropna(how="all")

    _df.LINE.fillna(_df.PAYEE, inplace=True)
    _df[['LINE', 'PAYEE']] = _df.LINE.str.split(" ", n=1, expand=True)

    _df.ACTUAL.fillna(_df["LINE DESCRIPTION"], inplace=True)
    _df[['ACTUAL', 'LINE DESCRIPTION']] = _df.ACTUAL.str.split(" ", n=1, expand=True)

    _df = _df.replace(["\)", ","], "", regex=True).replace("\(", "-", regex=True)
    _df.ACTUAL = _df.ACTUAL.astype(float)

    return _df

# %%
def read_purchase_order(file_obj, extension) -> pd.DataFrame:
    if extension == ".pdf":
        _df = read_pdf_purchase_order(file_obj)
    elif extension in [".xlsx", ".xlsb"]:
        _df = read_sheet(file_obj, extension)
    else:
        return pd.DataFrame()
    
    _df = _df.dropna(subset="PAYEE")
    
    try:
        return _df[['LINE', 'PAYEE', 'PO', 'DATE', 'PAYID', 'ACTUAL', 'LINE DESCRIPTION']]
    except:
        return pd.DataFrame()

# %% [markdown]
# ## Dropbox

# %%
class DbxDataRetriever:
    FILE_PREFERENCE = [".xlsx", ".xlsb", ".pdf"]

    datasets = {
        "CS" : [],
        "PR" : [],
        "PO" : []
    }

    def __init__(self, link, dbx) -> None:
        self.path = self.path_from_link(link)
        self.dbx = dbx

    def path_from_link(self, path):
        start_key = "sh/"

        if start_key in path:
            end = path.find("?")
        else:
            start_key = "home/"
            end = len(path)
        
        start = path.find(start_key) + len(start_key) - 1
        return path[start : end]
    
    def get_file(self, dbx_path):
        _meta, res = self.dbx.files_download(dbx_path)
        file_obj = res.content
        _type = classify_file(dbx_path, file_obj, verbose=True)
        extension = os.path.splitext(dbx_path)[1]

        return _type, extension, file_obj

    def file_to_df(self, _type:str, extension:str, file_obj:bytes) -> pd.DataFrame:
        if _type == "CS":
            return read_cost_summary(file_obj, extension)
        elif _type == "PR":
            return read_payroll(file_obj, extension)
        elif _type == "PO":
            return read_purchase_order(file_obj, extension)
        else:
            return pd.DataFrame()

    def ls_files_in_dir(self, path:str, _df=None) -> pd.DataFrame:
        res = self.dbx.files_list_folder(path)
        if _df is None:
            _df = pd.DataFrame(columns=["_type", "extension", "file_obj"])

        def process_entry(entry):
            file_path = entry.path_display
            if isinstance(entry, dropbox.files.FileMetadata):
                _df.loc[len(_df)] = self.get_file(file_path)
            elif isinstance(entry, dropbox.files.FolderMetadata):
                self.ls_files_in_dir(file_path, _df)


        with ThreadPoolExecutor() as executor:
            # Submit file processing tasks to the executor
            futures = [executor.submit(process_entry, entry) for entry in res.entries]

            # Wait for all tasks to complete
            wait(futures)

        return _df
    
    def select_best_file(self, _type:str, _df:pd.DataFrame):
        matches = _df[_df._type == _type]
        if matches.empty:
            return None

        for extension in self.FILE_PREFERENCE:
            matches = matches[matches.extension == extension]
            if not matches.empty:
                return matches.iloc[0].to_dict()
        
        return None
    
    def consolidate_dfs(self):
        for _type in self.datasets:
            if self.datasets.get(_type):
                self.datasets[_type] = pd.concat(self.datasets[_type])

    def gen_data(self):
        res = self.dbx.files_list_folder(self.path)

        def process_entry(entry):
            current_path = entry.path_display
            project_name = current_path.split("/")[-1]
            files = self.ls_files_in_dir(current_path, None)

            for _type in self.datasets:
                file = self.select_best_file(_type, files)
                if file:
                    _df = self.file_to_df(**file)
                    _df["PROJECT NAME"] = project_name
                    self.datasets[_type].append(_df)

        with ThreadPoolExecutor() as executor:
            # Submit file processing tasks to the executor
            futures = [executor.submit(process_entry, entry) for entry in res.entries]

            # Wait for all tasks to complete
            wait(futures)

        self.consolidate_dfs()