import pandas as pd
from pathlib import Path
import argparse
import datetime

# local helpers
from utils.io import get_dtypes_dict, read_meta_table
from utils.qcutils import reorder_table_to_CDE


def update_tables_to_CDEv2(tables_path: str|Path, CDEv1: pd.DataFrame, CDEv2: pd.DataFrame, out_dir: str|None = None ):
    """
    load the tables from the tables_path, and update them to the CDEv2 schema.  export the new tables to a datstamped out_dir
    """

    # Get the current date and time
    current_date = datetime.datetime.now()

   
    # Initialize the data types dictionary
    dtypes_dict = get_dtypes_dict(CDEv1)
        
    STUDY = read_meta_table(f"{tables_path}/STUDY.csv", dtypes_dict)
    PROTOCOL = read_meta_table(f"{tables_path}/PROTOCOL.csv", dtypes_dict)
    SUBJECT = read_meta_table(f"{tables_path}/SUBJECT.csv", dtypes_dict)
    CLINPATH = read_meta_table(f"{tables_path}/CLINPATH.csv", dtypes_dict)
    SAMPLE = read_meta_table(f"{tables_path}/SAMPLE.csv", dtypes_dict)

    # STUDY
    STUDYv2 = STUDY.copy() # don't really need to copy here
    assert len(SAMPLE['preprocessing_references'].unique()) == 1
    STUDYv2['preprocessing_references'] = SAMPLE['preprocessing_references'][0]
    if 'team_dataset_id' not in STUDYv2.columns:
        STUDYv2['team_dataset_id'] = STUDYv2['project_dataset'].str.replace(" ", "_").str.replace("-", "_")

    # PROTOCOL
    PROTOCOLv2 = PROTOCOL.copy()  

    SAMP_CLIN = SAMPLE.merge(CLINPATH, on="sample_id", how="left")
    SAMP_CLIN['source_sample_id'] = SAMP_CLIN['source_sample_id_x']
    SAMP_CLIN = SAMP_CLIN.drop(columns=['source_sample_id_x','source_sample_id_y'])

    SUBJ_SAMP_CLIN = SUBJECT.merge(SAMP_CLIN, on="subject_id", how="left")


    SUBJECT_cde_df = CDEv2[CDEv2['Table'] == "SUBJECT"]
    SUBJECT_cols = SUBJECT_cde_df["Field"].to_list()
    # SUBJECTv2 = SUBJ_SAMP_CLIN[SUBJECT_cols]
    SUBJECTv2 = SUBJ_SAMP_CLIN[SUBJECT_cols].drop_duplicates(inplace=False).reset_index()

    CLINPATH_cde_df = CDEv2[CDEv2['Table'] == "CLINPATH"]
    CLINPATH_cols = CLINPATH_cde_df["Field"].to_list()
    CLINPATHv2 = SUBJ_SAMP_CLIN[CLINPATH_cols]

    SAMPLE_cde_df = CDEv2[CDEv2['Table'] == "SAMPLE"]
    SAMPLE_cols = SAMPLE_cde_df["Field"].to_list()
    # SAMPLEv2 = SUBJ_SAMP_CLIN[SAMPLE_cols]
    SAMPLEv2 = SUBJ_SAMP_CLIN[SAMPLE_cols].drop_duplicates(inplace=False).reset_index()

    DATA_cde_df = CDEv2[CDEv2['Table'] == "DATA"]
    DATA_cols = DATA_cde_df["Field"].to_list()
    DATAv2 = SAMPLE[DATA_cols]


    STUDYv2 = reorder_table_to_CDE(STUDYv2, "STUDY", CDEv2)
    PROTOCOLv2 = reorder_table_to_CDE(PROTOCOLv2, "PROTOCOL", CDEv2)
    CLINPATHv2 = reorder_table_to_CDE(CLINPATHv2, "CLINPATH", CDEv2)
    SAMPLEv2 = reorder_table_to_CDE(SAMPLEv2, "SAMPLE", CDEv2)
    SUBJECTv2 = reorder_table_to_CDE(SUBJECTv2, "SUBJECT", CDEv2)
    DATAv2 = reorder_table_to_CDE(DATAv2, "DATA", CDEv2)

    
    # Format the date as a string in the format 'YYYYMMDD'
    date_str = current_date.strftime('%Y%m%d')

    tables_path = Path(tables_path)

    # write files to disk
    if out_dir is not None: 
        export_root = tables_path / f"{out_dir}_{date_str}"
        if not export_root.exists():
            export_root.mkdir(parents=True, exist_ok=True)


        STUDYv2.to_csv( export_root / "STUDY.csv")
        PROTOCOLv2.to_csv(export_root / "PROTOCOL.csv")
        SAMPLEv2.to_csv(export_root / "SAMPLE.csv")
        SUBJECTv2.to_csv(export_root / "SUBJECT.csv")
        CLINPATHv2.to_csv(export_root / "CLINPATH.csv")
        DATAv2.to_csv(export_root / "DATA.csv")

    return (STUDYv2, PROTOCOLv2, SAMPLEv2, SUBJECTv2, CLINPATHv2, DATAv2)


def read_CDEs(tab_path: str|Path):
    """Load CDEs from url (google sheet) or local csv a
    retern v2 and v1 CDEs as dataframes
    """
    # Construct the path to CSD.csv

    # google id for ASAP_CDE sheet
    GOOGLE_SHEET_ID = "1xjxLftAyD0B8mPuOKUp5cKMKjkcsrp_zr9yuVULBLG8"
    tab_path = Path(tab_path)

    sheet_name = "ASAP_CDE_v1"
    try:
        cde_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
        CDEv1 = pd.read_csv(cde_url)
    except:
        CDEv1 = pd.read_csv(tab_path / f"{sheet_name}.csv")
        print("read local file v1")

    sheet_name = "ASAP_CDE_v2"
    try:
        cde_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
        CDEv2 = pd.read_csv(cde_url)
    except:
        CDEv2 = pd.read_csv(tab_path / f"{sheet_name}.csv")
        print("read local file v2")

    return CDEv1, CDEv2

def main():
    # Set up the argument parser
    parser = argparse.ArgumentParser(description="A command-line tool to update tables from ASAP_CDEv1 to ASAP_CDEv2.")
    
    # Add arguments
    parser.add_argument("--tables", default=Path.cwd(),
                        help="Path to the directory containing meta TABLES. Defaults to the current working directory.")
    parser.add_argument("--cde", default=Path.cwd(),
                        help="Path to the directory containing CSD.csv. Defaults to the current working directory.")
    parser.add_argument("--outdir", default="v2",
                        help="Path to the directory containing CSD.csv. Defaults to the current working directory.")
    
    # Parse the arguments
    args = parser.parse_args()

    # CDE_path = args.cde / "ASAP_CDE_v1.csv" 
    # CDEv1 = pd.read_csv( Path.cwd() / "ASAP_CDE_v1.csv" )
    # CDEv2 = pd.read_csv( Path.cwd() / "ASAP_CDE_v2.csv" )

    CDEv1,CDEv2 = read_CDEs(args.cde)

    _ = update_tables_to_CDEv2(args.tables, CDEv1, CDEv2, args.outdir)



if __name__ == "__main__":
    main()

