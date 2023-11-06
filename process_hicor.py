""" Script for pulling HICOR data and converting it to RETAIN-compatible pickle files """
import argparse
import ast
from collections import defaultdict
import csv
from datetime import datetime
from io import StringIO
import json
import pandas as pd
import pickle
from pidcounter import PID_Counter
from sql_engine import initialize
from sqlalchemy import create_engine
from tqdm import tqdm
import urllib


CONVERTERS = {"numerics": ast.literal_eval, "codes": ast.literal_eval, "to_event": ast.literal_eval}

def make_engine():
    conn_str = (
        'Driver={SQL Server};'
        'Server=TIGRIS;'
        'Database=WorkDBTest;'
        'Trusted_Connection=yes;'
    )
    quoted_conn_str = urllib.parse.quote_plus(conn_str)

    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted_conn_str))
    return engine

class DataDictionary():
    def __init__(self, default_datadict=True, default_codes=True):
        self.code_cols = {}
        self.numeric_cols = []
        self.drop_cols = []
        if default_datadict:
            self.read_data_dict('./data_dictionary.csv')
        self.code_mappings = {}
        if default_codes:
            self.read_code_mapping('./dictionary.pkl')

    def read_data_dict(self, filepath):
        with open(filepath, "r") as fin:
            reader = csv.DictReader(fin)
            for row in reader:
                if (row['DataTreatment'] == 'Code'):
                    self.code_cols[row['Name']] = (row['DataTreatment'], row['DataType'], row['Temporal'])
                elif (row['DataTreatment'] == 'Numeric'):
                    self.numeric_cols.append(row['Name'])
                else:
                    self.drop_cols.append(row['Name'])

    def read_code_mapping(self, filepath):
        with open(filepath, "rb") as fin:
            code_map = pickle.load(fin)
        self.code_mappings = {v:k for k,v in code_map.items()}


def build_visit_dict(rowdict, datadict, error_cols, config):

    newvisit = {'PID': rowdict['PID'],
                'numerics': ([None]*len(datadict.numeric_cols)),
                'DAY': int(rowdict.get('DAY', 0)),
                'ED': int(rowdict.get('ED', 0)),
                'IP': int(rowdict.get('IP', 0))}
    codeset = set()
    for col, val in rowdict.items():
        codekey = datadict.code_cols.get(col)
        if codekey:
            try:
                mapped_code = None
                if (codekey[1] != 'Binary'):
                    mapped_code = datadict.code_mappings.get("{}_{}".format(col, val))
                elif int(val):
                    mapped_code = datadict.code_mappings.get(col)
                if mapped_code:
                    codeset.add(mapped_code)
            except:
                error_cols.add(col)
        elif col in datadict.numeric_cols:
            # change NULL values in numerics to valid default float given in config file
            # backoff to zero if column is not in config['DEFAULT_VALUES']
            if val is None:
                try:
                    val = config['DEFAULT_VALUES'][col]
                except:
                    print("Numeric columns must be non null or contained within the DEFAULT_VALUES \
                            dictionary in config.json - Column  ({}) defaulting to 0 ".format(col))
                    val = 0
            newvisit['numerics'][datadict.numeric_cols.index(col)] = val
    newvisit['codes'] = list(codeset)
    return newvisit


def get_partial_events(engine, config, datadict, pid, ARGS={}):
    event_types = ARGS.outcome or []
    for param in (engine, config, pid):
        if not param:
            raise ValueError("Var `{}` cannot be empty".format(param))
    acceptable_types = ["ED", "IP"]
    event_types = [etype.upper() for etype in event_types if etype]
    if not all(etype in acceptable_types for etype in event_types):
        raise ValueError("List `event_types` must be subset of ({})".format(", ".join(acceptable_types)))
    event_clause = " and ({})".format(" or ".join(["{} = 1".format(etype) for etype in event_types])) if event_types else ''
    partials = {}
    for row in engine.execute('select * from {} where PID = {}{}'.format(config['PARTIAL_EVENT_TABLE_NAME'], pid, event_clause)):
        rowdict = dict(row)
        newvisit = build_visit_dict(rowdict, datadict, set(), config)
        partials[newvisit['DAY']] = newvisit
    return partials

def get_all_previous_claims_days(engine, config, datadict, pid, event_day, error_cols, pid_counter, csv_writer):
    """
    get the full history of active claims day given a PID and final DAY in sequence
    """
    for row in engine.execute("select * from {} where PID = '{}' and ANYCLAIM!='0' and DAY < {} order by DAY"\
        .format(config['TABLE_NAME'], pid, event_day)):
        rowdict = dict(row)
        newvisit = build_visit_dict(rowdict, datadict, error_cols, config)
        pid_counter.pid_rows.append(newvisit)  
    return pid_counter


def get_event_clause(ARGS={}):
    # determine outcome variable for model
    event_types = ARGS.outcome or []
    acceptable_types = ["ED", "IP"]
    event_types = [etype.upper() for etype in event_types if etype]
    if not all(etype in acceptable_types for etype in event_types):
        raise ValueError("List `event_types` must be subset of ({})".format(", ".join(acceptable_types)))
    return " and ({})".format(" or ".join(["{} = 1".format(etype) for etype in event_types])) if event_types else ''


def split_on_events(results, config, datadict, output, csv_writer, ARGS={}):
    top_x = "top {} ".format(ARGS.max) if ARGS.max else ''
    event_clause = get_event_clause(ARGS)
    results = engine.execute('select {}* from {} where ANYCLAIM != 0 and PID is not null and PROCESS1={} '
                             'order by PID, DAY'.format(top_x, config['TABLE_NAME'], ARGS.dataset))

    error_cols = set()
    batch = results.fetchmany(config['WINDOW_SIZE'])
    batch_num = 0
    pid_counter = PID_Counter(preferred_types=ARGS.outcome)

    '''
    in order to split PID's with multiple events within their span of visits,
    we must hold on to all rows for a PID, and track when an event occurs. 
    when an event occurs:
     copy the current span of rows, changing the PID to a new namespace for all rows
    '''
    while batch:
        batch_num += 1
        print("Processing batch #{}:".format(batch_num))
        for row in tqdm(batch):
            rowdict = dict(row)
            newvisit = build_visit_dict(rowdict, datadict, error_cols, config)
            if pid_counter.is_new_pid(newvisit['PID']):
                if pid_counter.pid and pid_counter.pid_rows:
                    pid_counter.convert_and_write(csv_writer)
                pid_counter.reset_for_new_pid(newvisit['PID'])
                if ARGS.use_partials:
                    pid_counter.partial_events = get_partial_events(engine, config, datadict, newvisit['PID'], ARGS)
            pid_counter.process_row(newvisit)
            
        batch = results.fetchmany(config['WINDOW_SIZE'])
        #in the case that we have hit the end of our query, we need to write
        #the remaining rows out for processing
        if not batch:
            pid_counter.convert_rows_to_wide_rep()
            pid_counter.write_all_rows(csv_writer)

    if error_cols:
        print("Experienced errors with the following columns: {}".format(", ".join(list(error_cols))))
    return True


def split_on_matched_control(engine, config, datadict, negative_instances, output, csv_writer, ARGS={}):

    top_x = "top {} ".format(ARGS.max) if ARGS.max else ''
    event_clause = get_event_clause(ARGS)
    results = engine.execute('select {}* from {} where PID is not null and PROCESS1={} {}'
                .format(top_x, config['PARTIAL_EVENT_TABLE_NAME'], ARGS.dataset, event_clause))
    if negative_instances:
        print ('querying negative instances from full table')
        results = engine.execute('select {}* from {} where PID is not null and PROCESS1={} {} and ANYCLAIM=1'
            .format(top_x, config['TABLE_NAME'], ARGS.dataset, event_clause.replace('and', 'and not')))

    error_cols = set()
    batch = results.fetchmany(config['WINDOW_SIZE'])
    batch_num = 0
    pid_counter = PID_Counter(preferred_types=ARGS.outcome)
    events_by_day = defaultdict(list)
    print("Querying Positive Event Days...")
    while batch:
        batch_num += 1
        print("Processing batch #{}:".format(batch_num))
        for row in tqdm(batch):
            rowdict = dict(row)
            newvisit = build_visit_dict(rowdict, datadict, error_cols, config)
            # if querying by matched queries, each 'row' is an outcome event from the partials table
            event_day = rowdict['DAY']
            pid = rowdict['PID']
            pid_counter.reset_for_new_pid(pid)
            if not negative_instances:
                pid_counter.prev_event_value = 1
            else:
                pid_counter.prev_event_value = 0
            # find all previous days for that positive instance from the full claims table
            pid_counter = get_all_previous_claims_days(engine, config, datadict, pid, event_day, error_cols, pid_counter, csv_writer)
            pid_counter.convert_and_write(csv_writer, append=True, visit=newvisit, neg_only=False)

            events_by_day[event_day].append(pid)
        batch = results.fetchmany(config['WINDOW_SIZE'])


    print("Querying Negative Event Days...")
    for event_day, pid_list in tqdm(events_by_day.items()):
        # find N (match_num) random negative instances and then all previous days from the full claims table
        total_match_num = ARGS.match_num * len(pid_list)
        match_found = 0
        # changed by Lily. Remove PIDs who ever had positive events. 
        for row in engine.execute("select top {} * from {} where PID not in (select PID from {} where {}) and PID not in ('{}') and ANYCLAIM!='0' and DAY = {} \
                                and not {} and PROCESS1={} order by newid()".format(total_match_num, config['TABLE_NAME'],config['PARTIAL_EVENT_TABLE_NAME'], event_clause.strip(' and'),
                                "','".join(pid_list), event_day, event_clause.strip(' and'), ARGS.dataset)):
            match_found += 1
            rowdict = dict(row)
            newvisit = build_visit_dict(rowdict, datadict, error_cols, config)
            rand_pid = rowdict['PID']
            pid_counter.reset_for_new_pid(rand_pid)
            pid_counter = get_all_previous_claims_days(engine, config, datadict, rand_pid, event_day, error_cols, pid_counter, csv_writer)
            pid_counter.convert_and_write(csv_writer, append=True, visit=newvisit, neg_only=False)
        if match_found != total_match_num:
            print('Warning: missing ' + str(total_match_num-match_found) + ' matches for instance ' + pid)
    if error_cols:
        print("Experienced errors with the following columns: {}".format(", ".join(list(error_cols))))
    return True


def load_into_df(engine, config, datadict, ARGS={}):
    """
    Queries data from HICOR db, sorts/compiles into patient rows (split on events), and loads into DF.
    """
    print("Pulling {} entries from DB...".format('first {}'.format(ARGS.max) if ARGS.max else 'all'))
    output = StringIO()
    csv_writer = csv.DictWriter(output, fieldnames=config['HEADERS'], extrasaction='ignore')

    if ARGS.match_num or ARGS.day_instance:
        split_on_matched_control(engine, config, datadict, False, output, csv_writer, ARGS)
        if ARGS.day_instance:
            split_on_matched_control(engine, config, datadict, True, output, csv_writer, ARGS)
    else:
        split_on_events(engine, config, datadict, output, csv_writer, ARGS)

    output.seek(0)
    print("Loading into dataframe (this may take some time)...")
    return pd.read_csv(output, header=None, names=config['HEADERS'], converters=CONVERTERS)


def parse_arguments(parser):
    """parameters for the dataset (train,dev, test) to prepare, as well as the appropriate outcome variable"""
    parser.add_argument('--dataset', type=str, default='2',
                        help='dataset to prepare (1=train, 2=dev/validate, 3=test)')
    parser.add_argument('--outcome', type=str, default=None,
                        help='outcome variable in question (\'ed\' or \'ip\', defaults to both)')
    parser.add_argument('--max', type=int, default=None,
                        help="max number of visits to query from main DB")
    parser.add_argument('--trusted', action='store_true',
                        help="use trusted mssql connection (requires proper sql server drivers)")
    parser.add_argument('--use_partials', action='store_true',
                        help="query and include partial days on event-final claimdays")
    parser.add_argument('--match_num', type=int, default=0,
                        help="query data set by finding positive instances and matching with 'N' negative instances")
    parser.add_argument('--day_instance', action='store_true',
                        help="flag to indicate making a new instance for each new day (instead of each new positive outcome event)")
    args = parser.parse_args()
    args.outcome = [args.outcome] if args.outcome else ["ED", "IP"]

    return args


if __name__ == '__main__':

    start = datetime.now()
    PARSER = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ARGS = parse_arguments(PARSER)

    with open('./config.json') as fin:
        config = json.load(fin)
    
    ddict = DataDictionary()
    print("Processed Data Dictionary and Code Mapping files.")
    print("Total numerics: {}".format(len(ddict.numeric_cols)))

    engine = make_engine()

    df = load_into_df(engine, config, ddict, ARGS)
    print("Process complete. DF size: {}".format(df.shape))
    
    # final output file name is variable based on dataset and outcome variable
    if config.get('OUTPUT_FILEPATH'):
        print(config['OUTPUT_FILEPATH'])
        print(ARGS)
        target_file = "_".join([config['OUTPUT_FILEPATH'], "".join(ARGS.outcome), ARGS.dataset,'target.pkl'])
        data_file = "_".join([config['OUTPUT_FILEPATH'], "".join(ARGS.outcome), ARGS.dataset, 'data.pkl'])
        
        # sort by patients with the most "visits" first
        # len of codes and numerics should be the same (one list of features per "visit")
        df['visit_num'] = df['codes'].str.len()
        df = df.sort_values('visit_num', ascending=False)
        # keep the outcome variable as for the target file and lose the rest
        target_df = pd.DataFrame(df.pop('target'))
        df.to_pickle(data_file)
        target_df.to_pickle(target_file)
        print("DF data file created: {}".format(data_file))
        print("DF target file created: {}".format(target_file))
    else:
        print ('No output file path provided')

    print ('TOTAL PROCESSING TIME')
    print (datetime.now()-start)
