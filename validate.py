import argparse
import os
import pandas as pd
import numpy as np



class ReportCollector:
    def __init__(self):
        self.entries = []
        self.filename = None


    def add_markdown(self, msg):
        self.entries.append(("markdown", msg))

    def add_error(self, msg):
        self.entries.append(("error", msg))

    def add_header(self, msg):
        self.entries.append(("header", msg))

    def add_subheader(self, msg):
        self.entries.append(("subheader", msg))

    def add_divider(self):
        self.entries.append(("divider", None))
    
    def write_to_file(self, filename):
        self.filename = filename
        with open(filename, 'w') as f:
            report_content = self.get_log()
            f.write(report_content)
    

    def get_log(self):
        """ grab logged information from the log file."""
        report_content = []
        for msg_type, msg in self.entries:
            if msg_type == "markdown":
                report_content += msg + '\n'
            elif msg_type == "error":
                report_content += f"🚨⚠️❗ **{msg}**\n"
            elif msg_type == "header":
                report_content += f"# {msg}\n"
            elif msg_type == "subheader":
                report_content += f"\t## {msg}\n"
            elif msg_type == "divider":
                report_content += 60*'-' + '\n'
        
        return "".join(report_content)

    def reset(self):
        self.entries = []
        self.filename = None

    def print_log(self):
        print(self.get_log())


def get_log(log_file):
    """ grab logged information from the log file."""
    with open(log_file, 'r') as f:
        report_content = f.read()
    return report_content

def columnize( itemlist ):
    NEWLINE_DASH = ' \n- '
    if len(itemlist) > 1:
        return f"- {itemlist[0]}{NEWLINE_DASH.join(itemlist[1:])}"
    else:
        return f"- {itemlist[0]}"
    

# Function to read a table with the specified data types
def read_meta_table(table_path,dtypes_dict):
    # read the whole table
    try:
        table_df = pd.read_csv(table_path,dtype=dtypes_dict)
    except UnicodeDecodeError:
        table_df = pd.read_csv(table_path, encoding='latin1',dtype=dtypes_dict)

    # drop the first column if it is just the index
    if table_df.columns[0] == "Unnamed: 0":
        table_df = table_df.drop(columns=["Unnamed: 0"])
    return table_df

# Function to get data types dictionary for a given table
def get_dtypes_dict(cde_df):
    # unnescessary.
    # # Filter the CDE data frame to get the fields and data types for the specified table
    # table_cde = cde_df[cde_df["Table"] == table_name]
    
    # Initialize the data types dictionary
    dtypes_dict = {}
    
    # Iterate over the rows to fill the dictionary
    for _, row in cde_df.iterrows():
        field_name = row["Field"]
        data_type = row["DataType"]
        
        # Set the data type to string for "String" and "Enum" fields
        if data_type == "String":
            dtypes_dict[field_name] = str
        elif data_type == "Enum":
            dtypes_dict[field_name] = 'category'
        elif data_type == "Integer":
            dtypes_dict[field_name] = int
        elif data_type == "Float":
            dtypes_dict[field_name] = float
    
    return dtypes_dict



def validate_file(table_name, path, report):
    # Construct the path to CSD.csv
    try:
        cde_file_path = os.path.join(path, "ASAP_CDE.csv")
        if os.path.exists(cde_file_path):
            CDE_df = pd.read_csv(cde_file_path)
        else:
            cde_file_path = os.path.join(".", "ASAP_CDE.csv")
            CDE_df = pd.read_csv(cde_file_path)

    except:
        report.add_error(f"Could not find {table_name}.csv")
        return 0




    ref_file_path = os.path.join(path, f"{table_name}.csv")
    report.add_header(f"{table_name} table ({table_name}.csv)")

    reference_df = read_meta_table(ref_file_path,get_dtypes_dict(CDE_df)) 

    retval = validate_table(reference_df, table_name, CDE_df, report)

    return retval

def validate_table(table_in: pd.DataFrame, table_name: str, CDE: pd.DataFrame, out: ReportCollector):
    """
    Validate a table against the CDE, and log results to streamlit (outp="streamlit") or to a 
    log file (outp="logging" or both (outp="both"|"all")
    """

    retval = 1

    # Filter out rows specific to the given table_name from the CDE
    specific_cde_df = CDE[CDE['Table'] == table_name]
    table = prep_table(table_in, specific_cde_df)



    # Extract fields that have a data type of "Enum" and retrieve their validation entries
    enum_fields_dict = dict(zip(specific_cde_df[specific_cde_df['DataType'] == "Enum"]['Field'], 
                               specific_cde_df[specific_cde_df['DataType'] == "Enum"]['Validation']))
    
    # Extract fields that are marked as "Required"
    required_fields = specific_cde_df[specific_cde_df['Required'] == "Required"]['Field'].tolist()
    optional_fields = specific_cde_df[specific_cde_df['Required'] == "Optional"]['Field'].tolist()


    # Check for missing "Required" fields
    missing_required_fields = [field for field in required_fields if field not in table.columns]
    
    if missing_required_fields:
        out.add_error(f"Missing Required Fields in {table_name}: {', '.join(missing_required_fields)}")
    else:
        out.add_markdown(f"All required fields are present in *{table_name}* table.")

    # Check for empty or NaN values
    empty_fields = []
    total_rows = table.shape[0]
    for test_field,test_name in zip([required_fields, optional_fields], ["Required", "Optional"]):
        empty_or_nan_fields = {}
        for field in test_field:
            if field in table.columns:
                invalid_count = table[field].isna().sum()
                if invalid_count > 0:
                    empty_or_nan_fields[field] = invalid_count
                    
        if empty_or_nan_fields:
            out.add_error(f"{test_name} Fields with Empty (nan) values:")
            for field, count in empty_or_nan_fields.items():
                out.add_markdown(f"\n\t- {field}: {count}/{total_rows} empty rows")
            retval = 0
        else:
            out.add_markdown(f"No empty entries (<NA> or NaN) found in _{test_name}_ fields.")
    
    # Check for invalid Enum field values
    invalid_field_values = {}
    valid_field_values = {}

    invalid_fields = []
    invalid_nan_fields = []
    for field, validation_str in enum_fields_dict.items():
        valid_values = eval(validation_str)
        if field in table.columns:
            invalid_values = table[~table[field].isin(valid_values)][field].unique()
            if invalid_values.any():

                if 'Nan' in invalid_values:
                    invalid_nan_fields.append(field)
        
                invalids = [x for x in invalid_values if x != 'Nan' ]
                if len(invalids)>0:
                    invalid_fields.append(field)    
                    invalid_field_values[field] = invalids
                    valid_field_values[field] = valid_values
                


    if invalid_field_values:
        out.add_subheader("Enums")
        out.add_error("Invalid entries")
        # tmp = {key:value for key,value in invalid_field_values.items() if key not in invalid_nan_fields}
        # st.write(tmp)
        def my_str(x):
            return f"'{str(x)}'"
            
        for field, values in invalid_field_values.items():
            if field in invalid_fields:
                str_out = f"- _*{field}*_:  invalid values 💩{', '.join(map(my_str, values))}\n"
                str_out += f"    - valid ➡️ {', '.join(map(my_str, valid_field_values[field]))}"
                out.add_markdown(str_out)
                # out.add_markdown( f"- {field}: invalid values {', '.join(map(str, values))}" )
                # out.add_markdown( f"- change to: {', '.join(map(my_str, valid_field_values[field]))}" )

        if len(invalid_nan_fields) > 0:
            out.add_error("Found unexpected NULL (<NA> or NaN):")
            out.add_markdown(columnize(invalid_nan_fields))
        
        retval = 0

    else:
        out.add_subheader(f"Enum fields have valid values in {table_name}. 🥳")

    return retval

######## HELPERS ########
# Define a function to only capitalize the first letter of a string
def capitalize_first_letter(s):
    if not isinstance(s, str) or len(s) == 0:  # Check if the value is a string and non-empty
        return s
    return s[0].upper() + s[1:]


def prep_table(df_in:pd.DataFrame, CDE:pd.DataFrame) -> pd.DataFrame:
    """helper to force capitalization of first letters for string and Enum fields"""
    df = df_in.copy()
    string_enum_fields = CDE[CDE["DataType"].isin(["Enum", "String"])]["Field"].tolist()
    # Convert the specified columns to string data type using astype() without a loop
    columns_to_convert = {col: 'str' for col in string_enum_fields if col in df.columns}
    df = df.astype(columns_to_convert)
    for col in string_enum_fields:
        if col in df.columns and col not in ["assay", "file_type"]:
            df[col] = df[col].apply(capitalize_first_letter) 
    return df

def reorder_table_to_CDE(df: pd.DataFrame, df_name:str, CDE: pd.DataFrame) -> pd.DataFrame:
    """ convert table to CDE field order and create empty columns for missing fields"""
    col_order = CDE[CDE["Table"]==df_name].Field.tolist()
    
    df_out = pd.DataFrame()

    for col in col_order:
        if col in df.columns:   
            df_out[col] = df[col]
        else:
            df_out[col] = ""

    return df_out


def main():
    # Set up the argument parser
    parser = argparse.ArgumentParser(description="A command-line tool to validate against CSD.csv.")
    
    # Add arguments
    parser.add_argument("table", choices=["SAMPLE", "STUDY", "PROTOCOL", "CLINPATH", "SUBJECT", "CDE", "ALL"],
                        help="Specify the table for validation.")
    parser.add_argument("--path", default=os.getcwd(),
                        help="Path to the directory containing CSD.csv. Defaults to the current working directory.")
    parser.add_argument("--logfile", default="validation.log",
                        help="Name of the logfile. Defaults to 'validation.log'.")
    
    # Parse the arguments
    args = parser.parse_args()


    report = ReportCollector()

    # Call the validation function
    if args.table == "ALL":
        report.add_header(f"processing ALL tables")
        for table_name in ["SAMPLE", "STUDY", "PROTOCOL", "CLINPATH", "SUBJECT"]:
            retval = validate_file(table_name, args.path, report)
            report.add_divider()

    else:
        report.add_header(f"processing SINGLE table")
        retval = validate_file(args.table, args.path, report)
        report.add_divider()

    report.print_log()
    report.write_to_file(args.logfile)

if __name__ == "__main__":
    main()

