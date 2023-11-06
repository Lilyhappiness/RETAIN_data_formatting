import pandas as pd
import numpy as np
from sql_engine import initialize
import argparse
import json


def get_sql_counts(engine, ARGS):
    top_x = "top {} ".format(ARGS.max) if ARGS.max else ''
    # match the query in process_hicor.py so that the max arg can be used for faster querying and testing
    expected_total_pts = pd.read_sql_query('select {} PID from hicor_tester where PROCESS1 = {} order by PID, DAY'.format(top_x, ARGS.dataset), engine)
    print('\nexpected counts based on SQL query for dataset=={} and outcome=={}'.format(ARGS.dataset, ARGS.outcome))
    expected_total_pts = expected_total_pts.drop_duplicates()
    expected_total_pts['PID'] = expected_total_pts['PID'].astype(str)
    pid_str = '(' + ','.join([x for x in expected_total_pts['PID']]) + ')'

    
    # get all variable outcome days depending on outcome variable (ED, IP, or both)
    if len(ARGS.outcome) != 2:
        print ('defaulting to (ED|IP) for outcome variable')
        expected_total_events = pd.read_sql_query('select {} PID,DAY from hicor_outcome_event where PROCESS1 = {} and (IP=1 or ED=1) and PID in {}'.\
                                format(top_x, ARGS.dataset, pid_str), engine)
        max_any_claim_days = pd.read_sql_query('select {} PID, max(DAY) from hicor_tester where PROCESS1 = {} and (IP=0 and ED=0) and ANYCLAIM=1 AND PID in {} GROUP BY PID'.\
                                format(top_x, ARGS.dataset, pid_str), engine)

    else:
        expected_total_events = pd.read_sql_query('select {} PID,DAY from hicor_outcome_event where PROCESS1 = {} and {}=1 and PID in {}'.\
                                format(top_x, ARGS.dataset, ARGS.outcome, pid_str), engine)
        max_any_claim_days = pd.read_sql_query('select {} PID, max(DAY) from hicor_tester where PROCESS1 = {} and {}=0 and ANYCLAIM=1 AND PID in {} GROUP BY PID'.\
                                format(top_x, ARGS.dataset, ARGS.outcome, pid_str), engine)
    
    expected_total_events['PID'] = expected_total_events['PID'].astype(str)
    max_any_claim_days['PID'] = max_any_claim_days['PID'].astype(str)
    # get final variable outcome day for each patient
    max_outcome_days = expected_total_events.groupby('PID')['DAY'].max().reset_index().rename(columns={"DAY": "final_outcome_event"})
    # merge max claim days with max outcome days to keep & keep max claim if they are greater than final outcome days (or there are no outcome days)
    merged = max_any_claim_days.merge(max_outcome_days,  how="outer", on='PID')
    merged.columns = ['PID','DAY','final_outcome_event']
    no_outcomes = merged[merged.get("final_outcome_event").isnull()]
    final_zero_or_one_outcome = merged.loc[merged["DAY"] > merged["final_outcome_event"]]
    final_zero_or_one_outcome.pop('final_outcome_event');no_outcomes.pop('final_outcome_event')
    # concatenate the final claims days, with the total outcome events, with the no final claim days for no outcome patients
    final_expected_events = pd.concat([final_zero_or_one_outcome, expected_total_events, no_outcomes]).drop_duplicates()
    print (str(len(final_expected_events)) + ' total patient event sequences/instances in sql pull')
    print (str(len(expected_total_pts)) + ' unique patients in sql pull')

    return final_expected_events, expected_total_pts


def get_pickled_pandas(ARGS):
    print('\nobserved counts based on pickeld pandas dataframe for dataset=={} and outcome=={}'.format(ARGS.dataset, ARGS.outcome))
    actual_events = pd.read_pickle("_".join([config['OUTPUT_FILEPATH'], "".join(ARGS.outcome), ARGS.dataset, 'data.pkl']))
    actual_events['PID'] = actual_events['PID'].str.split('_', expand=True)
    actual_events['DAY'] = pd.to_numeric(actual_events['to_event'].apply(lambda x: x[-1]))
    actual_total_pts = actual_events['PID'].drop_duplicates().to_frame()
    actual_events.pop('numerics');actual_events.pop('codes')
    actual_events.pop('visit_num');actual_events.pop('to_event')
    print (str(len(actual_events)) + ' total patient event sequences/instances in dataframe')
    print (str(len(actual_total_pts)) + ' unique patients in dataframe')

    return actual_events, actual_total_pts


def parse_arguments(parser):
    """parameters for the dataset (train, dev, test) to prepare, as well as the appropriate outcome variable"""
    parser.add_argument('--dataset', type=str, default='2',
                        help='dataset to prepare (1=train, 2=dev/validate, 3=test)')
    parser.add_argument('--outcome', type=str, default=None,
                        help='outcome variable in question (\'ed\' or \'ip\', defaults to both)')
    parser.add_argument('--trusted', action='store_true',
                        help="use trusted mssql connection (requires proper sql server drivers)")
    parser.add_argument('--max', type=int, default=None,
                        help="max number of visits to query from main DB")
    args = parser.parse_args()
    # slightly different default value here so that we can pick up the pandas output 
    # automatically from hicor_process.py
    args.outcome = args.outcome if args.outcome else "EDIP"
    return args

def get_diff(diff_str, expected_df, actual_df, ARGS):
    '''
    compare distinct records through join, output data frame of mismatches
    '''
    merged = expected_df.merge(actual_df,  how="outer", indicator= "Comparison")
    matched = expected_df.merge(actual_df, how="inner", indicator= "Comparison")
    # create copy of dataframe with unique values from both sources
    mismatched_df = merged[merged.Comparison != "both"].copy(deep=True )
    #replace comparison category labels using dictionary
    mismatched_df["Comparison"].replace({"left_only": "IN SQL",
        "right_only":" IN DATAFRAME"}, inplace=True)
    print ('\n')
    print (str(len(matched)) + ' matched {} in SQL pull and pickled dataframe'.format(diff_str))
    print (str(len(mismatched_df)) + ' mismatched {} in SQL pull and pickled dataframe'.format(diff_str))
    mismatched_df.to_csv('mismatched_{}_{}_{}.csv'.format(diff_str, ARGS.dataset, ARGS.outcome))


if __name__ == '__main__':
    
    PARSER = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ARGS = parse_arguments(PARSER)
    print ('checking patient and "patient" counts for dataset=={} and outcome=={}'.format(ARGS.dataset, ARGS.outcome))
    with open('./config.json') as fin:
        config = json.load(fin)

    engine = initialize(config, ARGS.trusted)
    expected_total_events, expected_total_pts = get_sql_counts(engine, ARGS)
    actual_events, actual_total_pts = get_pickled_pandas(ARGS)
    # comment this out if you don't want output files with all mismatched records 
    get_diff('patients', expected_total_pts, actual_total_pts, ARGS)
    get_diff('events', expected_total_events, actual_events, ARGS)