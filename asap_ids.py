import pandas as pd
import json
# import ijson
from pathlib import Path


# Function to read a table with the specified data types
def read_meta_table(table_path,dtypes_dict):
    table_df = pd.read_csv(table_path,dtype=dtypes_dict, index_col=0)
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
        if data_type in ["String", "Enum"]:
            dtypes_dict[field_name] = str
    
    return dtypes_dict


STUDY_PREFIX = "ASAP_PMBDS_"
DATASET_ID = "ASAP_PMBDS"


def load_id_mapper(id_mapper_path:Path) -> dict:
    """ load the id mapper from the json file"""
    if Path.exists(id_mapper_path):
        with open(id_mapper_path, 'r') as f:
            id_mapper = json.load(f)
        print(f"id_mapper loaded from {id_mapper_path}")
    else:
        id_mapper = {}    
        print(f"id_mapper not found at {id_mapper_path}")
    return id_mapper
    

# # don't need the ijson version for now
# def load_big_id_mapper(id_mapper_path:Path, ids:list) -> dict:
#     """ load the id mapper from the json file"""
#     id_mapper = {}

#     if Path.exists(id_mapper_path):
#         with open(id_mapper_path, 'r') as f:
#             for k, v in ijson.kvitems(f, ''):
#                 if k in ids:
#                     id_mapper.update({k:v})    
#         print(f"id_mapper loaded from {id_mapper_path}")
#     else:
#         print(f"id_mapper not found at {id_mapper_path}")
            
#     return id_mapper

def write_id_mapper(id_mapper, id_mapper_path):
    """ write the id mapper to the json file"""
    # if Path.exists(id_mapper_path):
    #     mode = 'a'
    # else:
    #     mode = 'w'
    mode = 'w'
    with open(id_mapper_path, mode) as f:
        json.dump(id_mapper, f, indent=4)

    return 0


def generate_asap_subject_ids(subj_id_mapper:dict, 
                              subject_df:pd.DataFrame) -> tuple[dict, pd.DataFrame, int]:
    """
    generate new unique_ids for new subject_ids in subject_df table, 
    update the id_mapper with the new ids from the data table

    return the updated id_mapper, updated subject_df, and the starting index for the new ids
    """
    # extract the max value of the mapper's third (last) section ([2] or [-1]) to get our n
    if bool(subj_id_mapper):
        n = max([int(v.split("_")[2]) for v in subj_id_mapper.values() if v]) + 1
    else:
        n = 1
    nstart = n
    df_nodups_wids = subject_df.copy()
    # might want to use 'source_subject_id' instead of 'subject_id' since we want to find matches across teams
    # shouldn't actually matter but logically cleaner
    uids = [str(id) for id in df_nodups_wids['subject_id'].unique()]
    mapid = {}
    for uid in uids:
        mapid[uid]= n
        n += 1

    df_nodups_wids['uid_idx'] = df_nodups_wids['subject_id'].map(mapid)
    # make a new column with the ASAP_subject_id
    # and insert it at the beginning of the dataframe
    ASAP_subject_id = [f'{STUDY_PREFIX}{i:06}' for i in df_nodups_wids.uid_idx]
    df_nodups_wids.insert(0, 'ASAP_subject_id', ASAP_subject_id)

    df_nodups_wids['uid_idx_cumcount'] = df_nodups_wids.groupby('ASAP_subject_id').cumcount() + 1
    asap_id_mapper = dict(zip(df_nodups_wids['subject_id'], df_nodups_wids['ASAP_subject_id']))

    # update the subj_id_mapper
    subj_id_mapper.update(asap_id_mapper)
    
    return subj_id_mapper, df_nodups_wids, nstart



def generate_asap_sample_ids(subj_id_mapper:dict, 
                             sample_df:pd.DataFrame, 
                             nstart:int, 
                             samp_id_mapper:dict) -> tuple[dict, pd.DataFrame]:
    """
    generate new unique_ids for new sample_ids in sample_df table, 
    update the id_mapper with the new ids from the data table


    return the updated id_mapper and updated sample_df
    """
    # could pass subj_id_mapper as a parameter instead of n.  e.g.
    # if bool(subj_id_mapper):
    #     n = max([int(v.split("_")[2]) for v in subj_id_mapper.values() if v]) + 1
    # else:
    #     n = 1
    
    # since the current SAMPLE tables can have multipl sample_ids lets drop duplciates, with the caveat of replciates
    df_nodups = sample_df.drop_duplicates(subset=['sample_id'])
    
    # 
    uniq_subj = df_nodups.subject_id.unique()

    dupids_mapper = dict(zip(uniq_subj,
                        [num+nstart for num in range(len(uniq_subj))] ))

    df_dup_chunks = []
    for subj_id, samp_n in dupids_mapper.items():
        df_dups_subset = df_nodups[df_nodups.subject_id==subj_id].copy()
        asap_id = subj_id_mapper[subj_id]
        df_dups_subset['asap_sample'] = [f'{asap_id}_{samp_n:06}' for i in range(df_dups_subset.shape[0])]
        df_dups_subset['samp_rep_no'] = ['s'+str(i+1) for i in range(df_dups_subset.shape[0])]
        # make a new column with the asap_sample_id
        # and insert it at the beginning of the dataframe
        df_dups_subset['ASAP_sample_id'] = df_dups_subset['asap_sample'] + '_' + df_dups_subset['samp_rep_no']

        df_dup_chunks.append(df_dups_subset)
    df_dups_wids = pd.concat(df_dup_chunks)

    id_mapper = dict(zip(df_dups_wids.sample_id,
                        df_dups_wids.ASAP_sample_id))
    out_df = sample_df.copy()
    ASAP_sample_id = out_df['sample_id'].map(id_mapper)
    out_df.insert(0, 'ASAP_sample_id', ASAP_sample_id)

    samp_id_mapper.update(id_mapper)

    return samp_id_mapper, out_df



def process_meta_files(table_path, 
                       CDE_path, 
                       subject_mapper_path='subj_map.json', 
                       sample_mapper_path='samp_map.json', 
                       export_path = None):
    """
    read in the meta data table, generate new ids, update the id_mapper, write the updated id_mapper to file
    """

    try:
        subj_id_mapper = load_id_mapper(subject_mapper_path)
    except FileNotFoundError:
        subj_id_mapper = {}
        print(f"{subject_mapper_path} not found... starting from scratch")

    try:
        samp_id_mapper = load_id_mapper(sample_mapper_path)
    except FileNotFoundError:
        samp_id_mapper = {}
        print(f"{sample_mapper_path} not found... starting from scratch")

    if CDE_path.exists():
        CDE = pd.read_csv(CDE_path )
    else:
        print(f"{CDE_path} not found... aborting")
        return 0
    
    dtypes_dict = get_dtypes_dict(CDE)

    # add ASAP_team_id to the STUDY and PROTOCOL tables
    study_path = table_path / "STUDY.csv"
    if study_path.exists():
        study_df = read_meta_table(study_path, dtypes_dict)
        team_id = study_df['ASAP_team_name'].str.upper().replace('-', '_')
        study_df['ASAP_team_id'] = team_id
        # add ASAP_dataset_id = DATASET_ID to the STUDY tables
        study_df['ASAP_dataset_id'] = DATASET_ID
    else:
        study_df = None
        print(f"{study_path} not found... aborting")
        return 0

    protocol_path = table_path / "PROTOCOL.csv"
    if protocol_path.exists():
        protocol_df = read_meta_table(protocol_path, dtypes_dict)
        protocol_df['ASAP_team_id'] = team_id
    else:
        protocol_df = None
        print(f"{protocol_path} not found... aborting")
        return 0
    
    # add ASAP_subject_id to the SUBJECT tables
    subject_path = table_path / "SUBJECT.csv"
    if subject_path.exists():
        subject_df = read_meta_table(subject_path, dtypes_dict)
        subj_id_mapper, subject_df, n = generate_asap_subject_ids(subj_id_mapper, subject_df)
        # add ASAP_dataset_id = DATASET_ID to the SUBJECT tables
        subject_df['ASAP_dataset_id'] = DATASET_ID
    else:
        subject_df = None
        print(f"{subject_path} not found... aborting")
        return 0
    
    # add ASAP_sample_id and ASAP_dataset_id to the SAMPLE tables
    sample_path = table_path / "SAMPLE.csv"
    if sample_path.exists():
        sample_df = read_meta_table(sample_path, dtypes_dict)
        samp_id_mapper, sample_df = generate_asap_sample_ids(subj_id_mapper, sample_df, n, samp_id_mapper)
        sample_df['ASAP_dataset_id'] = DATASET_ID
    else:
        sample_df = None
        print(f"{sample_path} not found... aborting")
        return 0

    # add ASAP_sample_id to the CLINPATH tables
    clinpath_path = table_path / "CLINPATH.csv"
    if clinpath_path.exists():
        clinpath_df = read_meta_table(clinpath_path, dtypes_dict)
        clinpath_df['ASAP_sample_id'] = clinpath_df['sample_id'].map(samp_id_mapper)

    # once we update the CDE so CLINPATH has subject level data we can add this
    # # add ASAP_subject_id to the CLINPATH tables
    # clinpath_path = table_path / "CLINPATH.csv"
    # if clinpath_path.exists():
    #     clinpath_df = read_meta_table(clinpath_path, dtypes_dict)

    #     clinpath_df['ASAP_subject_id'] = clinpath_df['subject_id'].map(id_mapper)

    # export updated tables
    if export_path is not None:
        asap_tables_path = Path.cwd() / "ASAP_tables" / table_path.name.split("-")[1]
        print(f"exporting to {asap_tables_path}")
        if  not asap_tables_path.exists():
            asap_tables_path.mkdir()

        if study_path.exists():
            study_df.to_csv(asap_tables_path / study_path.name)
        if protocol_path.exists():
            protocol_df.to_csv(asap_tables_path / protocol_path.name)
        if subject_path.exists():
            subject_df.to_csv(asap_tables_path / subject_path.name)
        if sample_path.exists():
            sample_df.to_csv(asap_tables_path / sample_path.name)
        if clinpath_path.exists():
            clinpath_df.to_csv(asap_tables_path / clinpath_path.name)
    else:
        print("no ASAP_tables with ASAP_ID's exported")

    # write the updated id_mapper to file
    write_id_mapper(subj_id_mapper, subject_mapper_path)
    write_id_mapper(samp_id_mapper, sample_mapper_path)


    return 1



#########  script to generate the asap_ids.json file #####################
if __name__ == "__main__":

    ## get thd CDE to properly read in the meta data tables
    CDE_path = Path.cwd() / "ASAP_CDE.csv" 
    CDE = pd.read_csv(CDE_path )
    # Initialize the data types dictionary
    dtypes_dict = get_dtypes_dict(CDE)

    ## add team Lee
    table_root = Path.cwd() / "clean/team-Lee"
    subject_mapper_path = Path.cwd() / "ASAP_subj_map.json"
    sample_mapper_path = Path.cwd() / "ASAP_samp_map.json"
    export_root = Path.cwd() / "ASAP_tables" 
    
    process_meta_files(table_root, 
                       CDE_path, 
                       subject_mapper_path, 
                       sample_mapper_path, 
                       export_path=export_root)

    ## add team Hafler
    table_root = Path.cwd() / "clean/team-Hafler"
    process_meta_files(table_root, 
                       CDE_path, 
                       subject_mapper_path, 
                       sample_mapper_path, 
                       export_path=export_root)

    ## add team Hardy
    table_root = Path.cwd() / "clean/team-Hardy"
    process_meta_files(table_root, 
                       CDE_path, 
                       subject_mapper_path, 
                       sample_mapper_path, 
                       export_path=export_root)


