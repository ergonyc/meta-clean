# imports
import pandas as pd
import numpy as np

from .io import ReportCollector, columnize


def validate_table(table_in: pd.DataFrame, table_name: str, CDE: pd.DataFrame, out: ReportCollector):
    """
    Validate a table against the CDE, and log results to streamlit (outp="streamlit") or to a 
    log file (outp="logging" or both (outp="both"|"all")
    """

    retval = 1

    # Filter out rows specific to the given table_name from the CDE
    specific_cde_df = CDE[CDE['Table'] == table_name]
    
    # Extract fields that have a data type of "Enum" and retrieve their validation entries
    enum_fields_dict = dict(zip(specific_cde_df[specific_cde_df['DataType'] == "Enum"]['Field'], 
                               specific_cde_df[specific_cde_df['DataType'] == "Enum"]['Validation']))
    
    # Extract fields that are marked as "Required"
    required_fields = specific_cde_df[specific_cde_df['Required'] == "Required"]['Field'].tolist()
    optional_fields = specific_cde_df[specific_cde_df['Required'] == "Optional"]['Field'].tolist()

    # table returns a copy of the table with the specified columns converted to string data type
    table = force_enum_string(table_in, table_name, CDE)

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
            out.add_markdown(f"No empty entries (Nan) found in _{test_name}_ fields.")
    
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

        for field, values in invalid_field_values.items():
            if field in invalid_fields:
                out.add_markdown( f"\t- {field}:{', '.join(map(str, values))}" )
                out.add_markdown( f"\t>\t change to: {', '.join(map(str, valid_field_values[field]))}" )

        if len(invalid_nan_fields) > 0:
            out.add_error("Found unexpected NULL (nan):")
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

def force_enum_string(df_in:pd.DataFrame, df_name:str, CDE:pd.DataFrame) -> pd.DataFrame:
    """helper to force Enum columns to string data type, and force capitalization of first letters"""
    df = df_in.copy()
    string_enum_fields = CDE[(CDE["Table"] == df_name) & 
                                (CDE["DataType"].isin(["Enum", "String"]))]["Field"].tolist()
    # Convert the specified columns to string data type using astype() without a loop
    columns_to_convert = {col: 'str' for col in string_enum_fields if col in df.columns}
    df = df.astype(columns_to_convert)
    
    for col in string_enum_fields:
        if col in df.columns and col not in ["assay", "file_type"]:
            df[col] = df[col].apply(capitalize_first_letter)

    return df

def reorder_table_to_CDE(df: pd.DataFrame, df_name:str, CDE: pd.DataFrame) -> pd.DataFrame:
    """ convert table to CDE field order and create NULL (pd.NA) entries columns for missing fields"""
    col_order = CDE[CDE["Table"]==df_name].Field.tolist()
    
    df_out = pd.DataFrame()

    for col in col_order:
        if col in df.columns:   
            df_out[col] = df[col]
        else:
            df_out[col] = pd.NA # np.nan doesn't work because it converts the whole column to float
            print(f"WARNING: {col} not found in {df_name} table.  Adding NULL column.")
    return df_out


