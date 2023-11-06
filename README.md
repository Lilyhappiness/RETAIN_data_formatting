# HICOR_RetainDataFormatting

reformatting of HICOR claims data for the Emergency Department/Inpatient Stay project to assess risk through the retain model
this currently depends on pulling data from a SQL database on a test server managed by CIT

## Arguments 

--dataset  		the dataset to reformat (1=train, 2=dev/validate, 3=test).	defaults to 2

--outcome  		outcome variable in question (ed or ip).					defaults to both

--max 			max number of visits to query from main DB.					defaults to None

--trusted 		use trusted mssql connection (requires proper driver) 		defaults to None

--use_partials 	query and include partial days on event-final claimdays		defaults to None

--match_num		find positive instances and match with 'N'					defaults to None

--day_instance  make a new instance for each patient, for each new day		defaults to None


### Examples

in order to create the 1:10 matched controls data set for the training data in order to assess risk of Emergency Department visits and use partial data from the final day prior to ED event for all positive instances (assuming the appropriate sql server drivers are present):

>> python process_hicor.py --dataset 1 --outcome ed --trusted --use_partials --match_num 10


in order to create the initial dataset formatting, where all positive outcome instances were captured as well as the final negative day for all patients, for the dev/validation dataset, in order to assess the risk for Inpatient Stays but want to exclude the final outcome day entirely (assuming the user has sql server connection details in the config file):

>> python process_hicor.py --dataset 2 --outcome ip 

in order to create the "final test set"; the dataframe where each active day of claims for each patient is a new instance for ED risk and use partial data from the final day prior to ED event (assuming the appropriate sql server drivers are present):

>> python process_hicor.py --dataset 3 --outcome ed --trusted --day_instance --use_partials



### Notes

--max is useful for testing purposes, so that smaller data sets can be created to quickly test end to end dataset creation, and retain training and testing

'config_default.json' contains examples of variables and connection string details for sql server access. It must be replaced with a 'config.json' that contains the actual values


### Dependencies
Python 3.6

external librarys:
ast, pandas, sqlalchemy, tqdm
