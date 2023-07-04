from concurrent.futures import ThreadPoolExecutor, wait
from . import CONSTANTS
import pandas as pd
import numpy as np
import tempfile
import dropbox
import camelot
import pickle
import fitz
import os
import re


# HELPER FUNCTIONS ——————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————
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

def contains(string:str, contains:list) -> bool:
    for cont in contains:
        if cont in string:
            return True
    
    return False

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

def get_section_from_line(ln:int) -> str:
    try:
        ln = int(ln)
    except ValueError:
        return ln
    
    for section in CONSTANTS.SECTION_RANGES:
        if ln in CONSTANTS.SECTION_RANGES.get(section):
            return section
    
    return "OTHER"

def find_outliers_iqr(SERIES, threshold=1.5):
    q1 = SERIES.quantile(0.25)
    q3 = SERIES.quantile(0.75)
    iqr = q3 - q1

    cutoff = threshold * iqr
    lower_bound = q1 - cutoff
    upper_bound = q3 + cutoff

    outliers = SERIES[(SERIES < lower_bound) | (SERIES > upper_bound)]

    return outliers

def get_row_idx(_df:pd.DataFrame, key:str) -> int:
    try:
        return (_df == key).any(axis=1).idxmax()
    except ValueError:
        return 0

def camelot_read_pdf_bytes(file_obj, table_num=0) -> pd.DataFrame:
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_pdf:
        temp_pdf.write(file_obj)
        return camelot.read_pdf(temp_pdf.name)._tables[table_num].df.copy()

def read_sheet(file_obj, extension:str) -> pd.DataFrame:
    if extension == ".xlsx":
        _df = pd.read_excel(file_obj)
    elif extension == ".xlsb":
        _df = pd.read_excel(file_obj, engine='pyxlsb')
    
    start = get_row_idx(_df, "LINE")
    if not "ACTUAL" in _df.iloc[start]:
        _df.columns = _df.iloc[start].fillna(_df.iloc[start-1])
        end = _df[start:].isna().all(axis=1).idxmax()
        _df = _df.iloc[start+1 : end]
    else:
        _df.columns = _df.iloc[start]
        _df = _df.iloc[start+1]

    _df.dropna(subset=["LINE", "PAYEE"], inplace=True)
    _df = _df.replace(["\)", ","], "", regex=True).replace("\(", "-", regex=True)

    if "RATE" in _df.columns:
        _df.RATE = _df.RATE.astype(float)
    
    return _df

def replaced(_list:list, idxs:list, values:list) -> list:
    ret_list = _list.copy()
    for idx, value in zip(idxs, values):
        ret_list[idx] = value

    return ret_list


# COST SUMMARY FUNCTIONS ————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————
def read_hot_budget_cs(file_obj, extension) -> pd.DataFrame:
    if extension == ".pdf":
        _df = camelot_read_pdf_bytes(file_obj, 1)
        
        _df.drop(12, inplace=True)

        _df.columns = CONSTANTS.HB_CS_COLS
        _df.drop(columns=["drop"], inplace=True)
        _df = _df.loc[1:]

        _df = _df.replace([r"CS\d+\b ", r".*\n", "\)"], "", regex=True).replace("\(", "-", regex=True)

        _df[_df.columns[1:]] = _df.iloc[:, 1:].replace("", np.nan).apply(lambda x: x.str.replace(',', '')).astype(float)

        _df = _df.dropna(thresh=2)

        return _df.reset_index(drop=True)
    elif extension == ".xlsx":
        _df = pd.read_excel(file_obj)

        date_pattern = r'[A-Za-z]+\s+\d{1,2},\s+\d{4}'
        date_match = re.search(date_pattern, _df.columns[0])

        # Extract the matched date
        if date_match:
            date = str(pd.to_datetime(date_match.group(0)).date())
        else:
            date = "REPLACE"


        start = get_row_idx(_df, "ESTIMATED COST SUMMARY")
        _df.columns = _df.iloc[start]
        _df = _df.iloc[start+1: start + 24]

        dir_cost = get_row_idx(_df, "Direct Costs A - K")
        if dir_cost:
            _df.drop(dir_cost, inplace=True)
        
        _df = _df.dropna(how="all", axis=1).drop(11).dropna(thresh=3).rename(columns={"ESTIMATED COST SUMMARY":"SECTION"})
        _df.drop(_df.columns[1], axis=1, inplace=True)

        sep_nums = lambda x: x[re.search(r"\d ", x).end():]
        _df.SECTION = _df.SECTION.apply(sep_nums)
        _df["DATE"] = date

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

def clean_SECTION(val:str) -> str:
    val = val.strip()

    if "Production Fee" in val:
        val = "Production Fee"
    elif "Insurance" in val:
        val = "Insurance"
    elif "Talent Exp r" in val:
        val = "Talent Expenses"
    
    return val.upper()

def read_cost_summary(file_obj, extension) -> pd.DataFrame:
    content = get_content(extension, file_obj)
    
    if "ESTIMATED COST SUMMARY" in content:
        _df = read_hot_budget_cs(file_obj, extension)
    elif "Film Production Cost Summary" in content:
        _df = read_GetActual_cs(file_obj)
    else:
        return pd.DataFrame()
    
    _df.fillna(0, inplace=True)
    _df.SECTION = _df.SECTION.apply(clean_SECTION)
    
    _df["VARIANCE (%)"] = _df["VARIANCE"] / (_df["BID TOTALS"] + 1E-5) * 100

    for section in _df["SECTION"].unique():
        section_df = _df[_df["SECTION"] == section]
        outliers = find_outliers_iqr(section_df["VARIANCE (%)"])
        _df.loc[outliers.index, "VARIANCE (%)"] = section_df["VARIANCE (%)"].median()

    return _df


# CS SUBSECTION FUNCTIONS ———————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————
def clean_xlsx_section_df(section_df, section) -> pd.DataFrame:
    start = get_row_idx(section_df, section)
    section_df.columns = section_df.iloc[start]
    section_df = section_df.iloc[start+1:].reset_index(drop=True)
    section_df = section_df[:get_row_idx(section_df, "SUB TOTAL")]
    
    section_df = section_df[replaced(CONSTANTS.CS_SUBSECTION_COLS, [0, 1], [section_df.columns[0], section])]
    section_df.columns = CONSTANTS.CS_SUBSECTION_COLS
    section_df = section_df.dropna(thresh=3).reset_index(drop=True).fillna(0.0)
    
    section_df.ACTUAL = pd.to_numeric(section_df.ACTUAL, errors="coerce")
    section_df.dropna(inplace=True)
    
    section_df.insert(0, "SECTION", section)
    section_df["VARIANCE"] = section_df["ACTUAL"] - section_df["ESTIMATE"]
    section_df["VARIANCE (%)"] = section_df["VARIANCE"] / (section_df["ESTIMATE"] + 1E-5)

    return section_df

def get_HB_xlsx_secion_dfs(cs, file_obj) -> pd.DataFrame:
    section_dfs = []
    _df = pd.read_excel(file_obj, header=37)

    for section in cs.SECTION.unique():
        try:
            section_dfs.append(clean_xlsx_section_df(_df.copy(), section))
        except:
            continue

    return pd.concat(section_dfs, ignore_index=True)

def to_read(sections:list):
    _to_read = {}

    for section in sections:
        info = CONSTANTS.HB_PDF_SECTION_LOCS.get(section)
        if not info:
            continue
        page = info[0]
        table = info[1]

        if not page in _to_read:
            _to_read[page] = []
        
        _to_read.get(page).append(table)
    
    return _to_read

def clean_pdf_section_df(section_df) -> pd.DataFrame:
    start = get_row_idx(section_df, "ACTUAL") or 0
    section_df.columns = section_df.iloc[start]
    section_df = section_df.iloc[start+1:].reset_index(drop=True)
    section_df = section_df[:get_row_idx(section_df, "SUB TOTAL")]
    section = section_df.columns[1]

    section_df = section_df[replaced(CONSTANTS.CS_SUBSECTION_COLS, [0, 1], [section_df.columns[0], section])]
    section_df.columns = CONSTANTS.CS_SUBSECTION_COLS

    section_df["ESTIMATE"] = section_df["ESTIMATE"].str.replace(',', '')
    section_df["ACTUAL"] = section_df["ACTUAL"].str.replace(',', '')

    section_df[["DAYS", "RATE", "ESTIMATE", "ACTUAL"]] = section_df[["DAYS", "RATE", "ESTIMATE", "ACTUAL"]].apply(pd.to_numeric, errors="coerce")
    section_df = section_df.dropna(subset="ACTUAL").reset_index(drop=True).fillna(0.0)

    section_df.insert(0, "SECTION", section)
    section_df["VARIANCE"] = section_df["ACTUAL"] - section_df["ESTIMATE"]
    section_df["VARIANCE (%)"] = section_df["VARIANCE"] / (section_df["ESTIMATE"] + 1E-5)


    return section_df

def get_HB_pdf_section_dfs(cs, file_obj):
    section_dfs = []

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_pdf:
        temp_pdf.write(file_obj)
        for page_num, table_nums in to_read(cs.SECTION.unique()).items():
            for table in camelot.read_pdf(temp_pdf.name, pages=str(page_num))._tables:
                if table.order in table_nums:
                    section_dfs.append(clean_pdf_section_df(table.df))

    return pd.concat(section_dfs, ignore_index=True)

def get_CS_section_dfs(cs, file_obj, extension) -> pd.DataFrame:
    section_dfs = None

    if "xlsx" in extension:
        section_dfs = get_HB_xlsx_secion_dfs(cs, file_obj)
    elif "pdf" in extension:
        section_dfs = get_HB_pdf_section_dfs(cs, file_obj)
    else:
        return pd.DataFrame()
    
    section_dfs = section_dfs.replace(r"\s{2,}.*", "", regex=True)
    section_dfs["DATE"] = cs.DATE[0]

    return section_dfs


# PAYROLL FUNCTIONS —————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————
def read_pdf_payroll(file_obj) -> pd.DataFrame:
    _df = camelot_read_pdf_bytes(file_obj, 0)
    
    _df.columns = CONSTANTS.PR_COLS
    _df = _df.iloc[1:].reset_index(drop=True).replace("", np.nan).dropna(how="all")

    _df.LINE.fillna(_df.PAYEE, inplace=True)
    _df[['LINE', 'PAYEE']] = _df.LINE.str.split(" ", n=1, expand=True)

    _df = _df.replace(["\)", ","], "", regex=True).replace("\(", "-", regex=True)
    _df.ACTUAL = _df.ACTUAL.astype(float)

    return _df

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
    _df["VARIANCE (%)"] = _df.VARIANCE / _df.EST
    _df["SECTION"] = _df.LINE.apply(get_section_from_line)

    return _df[["LINE", "SECTION", "PAYEE", "RATE", "EST", "ACTUAL", "VARIANCE", "VARIANCE (%)", "LINE DESCRIPTION"]].fillna(0.0)


# PURCHASE ORDER LOG FUNCTIONS ——————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————
def read_pdf_purchase_order(file_obj) -> pd.DataFrame:
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_pdf:
        temp_pdf.write(file_obj)
        _df = camelot.read_pdf(temp_pdf.name)._tables[0].df.copy()
    
    _df.columns = CONSTANTS.PO_COLS
    _df = _df.iloc[1:].reset_index(drop=True).replace("", np.nan).dropna(how="all")

    _df.LINE.fillna(_df.PAYEE, inplace=True)
    _df[['LINE', 'PAYEE']] = _df.LINE.str.split(" ", n=1, expand=True)

    _df.ACTUAL.fillna(_df["LINE DESCRIPTION"], inplace=True)
    _df[['ACTUAL', 'LINE DESCRIPTION']] = _df.ACTUAL.str.split(" ", n=1, expand=True)

    _df = _df.replace(["\)", ","], "", regex=True).replace("\(", "-", regex=True)

    return _df

def clean_payee(payee) -> str:
    payee = payee.split("-")[0].lower()

    for sub in CONSTANTS.subs:
        payee = re.sub(sub[0], sub[1], payee)
    payee = payee.strip()
    
    for cat in CONSTANTS.categories:
        if contains(payee, cat[0]):
            payee = cat[2]
            break
        elif payee in cat[1]:
            payee = cat[2]
            break

    return payee.strip().title()

def read_purchase_order(file_obj, extension) -> pd.DataFrame:

    if extension == ".pdf":
        _df = read_pdf_purchase_order(file_obj)
    elif extension in [".xlsx", ".xlsb"]:
        _df = read_sheet(file_obj, extension)
    else:
        return pd.DataFrame()
    
    _df.ACTUAL = pd.to_numeric(_df.ACTUAL, errors="coerce").astype(float)
    _df.LINE = pd.to_numeric(_df.LINE, errors="coerce")
    _df = _df.dropna(subset=["LINE", "ACTUAL", "PAYEE"])
    
    _df["SECTION"] = _df.LINE.apply(get_section_from_line)
    _df["LINE DESCRIPTION"] =_df["LINE DESCRIPTION"].fillna("NA")

    _df["DATE"] = _df["DATE"].fillna(_df["DATE"].median()).dt.date.astype(str)

    _df.PAYEE = _df.PAYEE.apply(clean_payee)
    
    try:
        return _df[['LINE', 'SECTION', 'PAYEE', 'DATE', 'ACTUAL', 'LINE DESCRIPTION']]
    except:
        return pd.DataFrame()

# DBX RETRIEVER CLASS ———————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————
class DbxDataRetriever:
    cache_path = "dbx_retrieval_cache.pickle"
    df_caches_path = "df_caches"

    def __init__(self, link, dbx, clear_cache=False) -> None:
        self.path = self.path_from_link(link)
        self.dbx = dbx
        self.dbx_files = {}
        self.cache = {}
        self.datasets = {
            "CS" : [],
            "CSSS" : [],
            "PR" : [],
            "PO" : []
        }

        if not os.path.isdir(self.df_caches_path):
            os.mkdir(self.df_caches_path)
        
        if clear_cache:
            self.clear_cache()
        else:
            self.load_cache()
        
    def clear_cache(self) -> None:
        self.cache = {}
        self.save_cache()
        for file in os.listdir(self.df_caches_path):
            os.remove(os.path.join(self.df_caches_path, file))

    def path_from_link(self, path) -> str:
        start_key = "sh/"
        end = path.find("?")
        if end == -1:
            end = len(path)

        if not start_key in path:
            start_key = "home/"
        
        start = path.find(start_key) + len(start_key) - 1
        return path[start : end]

    def load_cache(self) -> None:
        if os.path.exists(self.cache_path):
            with open(self.cache_path, "rb") as f:
                self.cache = pickle.load(f)
    
    def save_cache(self) -> None:
        with open(self.cache_path, 'wb') as f:
            pickle.dump(self.cache, f)
    
    def cache_and_check(self, metadata) -> bool:
        '''
        Returns True if the given file hase been previously cached, 
        and False if it has not been cached previously. It also 
        caches the file if not.
        '''
        file_name = metadata.name
        date = str(metadata.client_modified)
        cached_date = self.cache.get(file_name)

        if cached_date == date:
            return True
        else:
            self.cache[file_name] = date
            return False

    def get_file(self, dbx_path):
        _meta, res = self.dbx.files_download(dbx_path)
        file_obj = res.content
        _type = classify_file(dbx_path, file_obj, verbose=False)
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

    def ls_files_in_dir(self, path:str, entries=[]) -> list:
        res = self.dbx.files_list_folder(path)
        cache_check = []

        def process_entry(entry):
            file_path = entry.path_display
            if isinstance(entry, dropbox.files.FileMetadata):
                cache_check.append(self.cache_and_check(entry))
                entries.append(entry)
            elif isinstance(entry, dropbox.files.FolderMetadata):
                self.ls_files_in_dir(file_path, entries)
        
        with ThreadPoolExecutor() as executor:
            # Submit file processing tasks to the executor
            futures = [executor.submit(process_entry, entry) for entry in res.entries]

            # Wait for all tasks to complete
            wait(futures)
        
        if False in cache_check:
            return entries
        else:
            return None

    def create_files(self) -> None:
        res = self.dbx.files_list_folder(self.path)

        def process_entry(entry):
            if isinstance(entry, dropbox.files.FolderMetadata):
                dir_files = self.ls_files_in_dir(entry.path_display)
                if dir_files:
                    self.dbx_files[entry.name] = dir_files


        with ThreadPoolExecutor() as executor:
            # Submit file processing tasks to the executor
            futures = [executor.submit(process_entry, entry) for entry in res.entries]

            # Wait for all tasks to complete
            wait(futures)

    def get_files_from_project(self, entries, _df) -> pd.DataFrame:
        if _df is None:
            _df = pd.DataFrame(columns=["_type", "extension", "file_obj"])

        def process_entry(entry):
            file_path = entry.path_display
            if isinstance(entry, dropbox.files.FileMetadata):
                _df.loc[len(_df)] = self.get_file(file_path)


        with ThreadPoolExecutor() as executor:
            # Submit file processing tasks to the executor
            futures = [executor.submit(process_entry, entry) for entry in entries]
            # Wait for all tasks to complete
            wait(futures)

        return _df
    
    def select_best_file(self, _type:str, _df:pd.DataFrame):
        matches = _df[_df._type == _type]
        if matches.empty:
            return None

        for extension in CONSTANTS.FILE_PREFERENCE:
            matches = matches[matches.extension == extension]
            if not matches.empty:
                return matches.iloc[0].to_dict()
        
        return None
    
    def cache_df(self, df, _type):
        path = os.path.join(self.df_caches_path, "%s.csv" % _type)
        if not os.path.exists(path):
            df.to_csv(path, index=False)
            return df

        cache_df = pd.read_csv(path)
        if not df.empty:
            cache_df = cache_df[~cache_df['PROJECT NAME'].isin(df["PROJECT NAME"].unique())]
            cache_df = pd.concat([cache_df, df])

        cache_df.to_csv(path, index=False)

        return cache_df


    def consolidate_datasets(self) -> None:
        for _type in self.datasets:
            if self.datasets.get(_type):
                df = pd.concat(self.datasets[_type], ignore_index=True).reset_index(drop=True)

                if _type in ["CS", "PR"]:
                    for section in df["SECTION"].unique():
                        section_df = df[df["SECTION"] == section]
                        outliers = find_outliers_iqr(section_df["VARIANCE (%)"])
                        df.loc[outliers.index, "VARIANCE (%)"] = section_df["VARIANCE (%)"].median()
                elif _type == "CSSS":
                    for section in df["SUB SECTION"].unique():
                        section_df = df[df["SUB SECTION"] == section]
                        outliers = find_outliers_iqr(section_df["VARIANCE (%)"])
                        df.loc[outliers.index, "VARIANCE (%)"] = section_df["VARIANCE (%)"].median()
                elif _type != "PO":
                    df["VARIANCE (%)"] = df["VARIANCE (%)"].clip(upper=100)

                self.datasets[_type] = self.cache_df(df, _type)
            else:
                self.datasets[_type] = self.cache_df(pd.DataFrame(), _type)
    
    def create_datasets(self) -> None:
        self.create_files()

        def process_project(dir):
            project_name = dir
            entries = self.dbx_files[dir]
            
            files = self.get_files_from_project(entries, None) # pd.DataFrame of fileobjs and their descriptors

            for _type in self.datasets:
                file = self.select_best_file(_type, files)
                if file:
                    _df = self.file_to_df(**file)
                    _df["PROJECT NAME"] = project_name
                    self.datasets[_type].append(_df)

                    date_str = "20%s-01-01" % project_name[:2]
                    if _type == "CS":
                        _df.DATE = _df.DATE.replace("REPLACE", date_str)

                        csss = get_CS_section_dfs(_df, file["file_obj"], file["extension"])
                        csss["PROJECT NAME"] = project_name
                        self.datasets["CSSS"].append(csss)

        with ThreadPoolExecutor() as executor:
            # Submit file processing tasks to the executor
            futures = [executor.submit(process_project, dir) for dir in self.dbx_files.keys()]
            # Wait for all tasks to complete
            wait(futures)

        self.consolidate_datasets()
        self.save_cache()