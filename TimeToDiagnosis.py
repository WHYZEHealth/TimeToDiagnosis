import pyodbc
import time
import textwrap
import pandas as pd
import os,sys
from pathlib import Path
import configparser
    


path = Path(__file__)
ROOT_DIR = path.parent.absolute()
config_path  = os.path.join(ROOT_DIR, "config.ini")
config  = configparser.ConfigParser()
config.read(config_path)

username = config.get('Medications', 'username')
driver = config.get('Medications', 'driver')
server = config.get('Medications', 'server')
password = config.get('Medications', 'password')
database = config.get('Medications', 'database')

DataDictionaryRawData = pd.read_csv("DiagnosisDataDictionary.csv")

connection_string = config.get('Medications', 'connection_string').replace("'", "")

if connection_string == '':
    connection_string = textwrap.dedent('''
        Driver={driver};
        Server={server};
        Port = 1433;
        Database={database};
        Uid={username};
        Pwd={password};
        Encrypt=yes;
        TrustServerCertificate=no;
        Connection Timeout=30;
    '''.format(
        driver=driver,
        username=username,
        password=password,
        database=database,
        server=server
    )).replace("'", "")


#Create a new connection

try:
    conn: pyodbc.Connection = pyodbc.connect(connection_string)
except pyodbc.OperationalError:
    time.sleep(2)
    conn: pyodbc.Connection = pyodbc.connect(connection_string)

sql_query = '''/****** Script for SelectTopNRows command from SSMS  ******/
select b.*,
case when total_days > 79 then
total_days
else
0
end red,
case when total_days between 59 and 79 then
total_days
else
0
end amber,
case when total_days < 59 then
total_days
else
0
end green
from(select *, CONCAT(StartDateString,' to ', EndDateString) as DateRange,
cast((MRIReportDuration + ConsulationDuration + BiopsyDuration + MRIToBiopsyDuration + BiopsyReviewDuration) as float) as TotalDiagnosisTime,
DATEDIFF(day, StartDate, EndDate) as total_days
from 
(select [PM ID]as PatientNo,
CONVERT(VARCHAR(11),cast([start_date] as date), 106)  AS StartDateString,
cast([start_date] as date) as StartDate,
[Referral_yes_no] as ReferralBool,
cast([Duration for MRI report] as float) as MRIReportDuration,
cast([Days for Consultation] as float) as ConsulationDuration,
cast([Biopsy Duration] as float) as BiopsyDuration,
cast([MRI To Bio Duration] as float) as MRIToBiopsyDuration,
cast([Biopsy review duration] as float) as BiopsyReviewDuration,
[Consulation_yes_no] as ConsultationBool,
cast([end_date] as date) as EndDate ,
CONVERT(VARCHAR(11),cast([end_date] as date), 106)  AS EndDateString,

[Affected by covid] as AffectedByCovid
from [dbo].[time_to_diagnosis_cleaned_data_csv]
)a)b
ORDER BY StartDate asc
'''

data_df = pd.read_sql_query(sql_query, conn)

######## Data Dictionary

DataDictionaryRawData.columns = ["Label","Description","TextFormat"]
DataDictionarydictRecords = DataDictionaryRawData.to_dict('records')

class TimeToDiagnosis:
    def referral_data_structure(data_df):

        data_df['year'] = pd.DatetimeIndex(data_df['StartDate']).year
        data_df['year'] = data_df['year'].astype(str)
        result = {'XAxis': list(data_df['PatientNo'].values),
                   'XAxisDates': list(data_df['year']), 
                  'Categories': [
                        {'Data': list(data_df['MRIReportDuration']), 'Label': 'MRI'},
                        {'Data': list(data_df['ConsulationDuration']), 'Label': 'Consultant Appointment'},
                        {'Data': list(data_df['BiopsyDuration']), 'Label': 'Biopsy Procedure'},
                        {'Data': list(data_df['MRIToBiopsyDuration']), 'Label': 'Biopsy Booked'},
                        {'Data': list(data_df['BiopsyReviewDuration']), 'Label': 'Biopsy Review'}
            ]}
        return result

    def diagnosis(data_df):
    
        sorted_df = data_df.sort_values(by = ['total_days'], ascending =False)
        result = {
            'XAxis': list(sorted_df['PatientNo'].values),
            'XAxisDates': list(sorted_df['DateRange'].values),
            'Categories': [
                        {'Data': list(sorted_df['red']), 'Label': '>79'},
                        {'Data': list(sorted_df['amber']), 'Label': '<80 to >58'},
                        {'Data': list(sorted_df['green']), 'Label': '<59'},

            ]} 
        return result


    def AnnualAverage(data_df):
        data_df['year'] = pd.DatetimeIndex(data_df['StartDate']).year
        new_df = data_df[['year', 'total_days', 'PatientNo']]
        
        grouped_df = new_df.groupby('year') \
        .agg({'PatientNo':'size', 'total_days':'mean'}) \
        .rename(columns={'PatientNo':'count','total_days':'mean'}) \
        .reset_index()
        
        grouped_df['year'] = grouped_df['year'].astype(str)
        grouped_df = grouped_df.round(2)
        result = {
            'XAxis': list(grouped_df['year']),
            'NumberOfPatients': list(grouped_df['count']), 'Data': list(grouped_df['mean'])
                } 

        return result



class TimeToDiagnosisCharts:
    def restructure(self):
        covid = data_df[data_df['AffectedByCovid'] == 'Yes']
        not_covid = data_df[data_df['AffectedByCovid'] == 'No']
        all_average_time_to_diagnosis = round(data_df['total_days'].mean(), 2)
        covid_average_time_to_diagnosis = round(covid['total_days'].mean(), 2)
        not_covid_average_time_to_diagnosis = round(not_covid['total_days'].mean(), 2)
        all_registered_patients = len(data_df)
        covid_registered_patients = len(covid)
        not_covid_registered_patients = len(not_covid)

        final_output = {
            'NotCovidAverageTimeToDiagnosis': int(not_covid_average_time_to_diagnosis),
            'NotCovidPatients' : int(not_covid_registered_patients),
            'CovidPatients' : int(covid_registered_patients),
            'CovidAverageTimeToDiagnosis': int(covid_average_time_to_diagnosis),
            'AllAverageTimeToDiagnosis' : int(all_average_time_to_diagnosis),
            'AllRegisteredPatients' : int(all_registered_patients),

        }

        covid_data = data_df[data_df['AffectedByCovid']=='Yes']
        not_covid_data = data_df[data_df['AffectedByCovid']=='No']   
        
        
        Referrals = { 
            'AvgPublic': 79,
            'AvgPrivate': 59,
            'CovidData': TimeToDiagnosis.referral_data_structure(covid_data),
            'NotCovidData':TimeToDiagnosis.referral_data_structure(not_covid_data),
            'AllData': TimeToDiagnosis.referral_data_structure(data_df)
            }

        final_output['Referrals'] = Referrals

        Diagnosis =  {
                'AvgPublic' : 79,
                'AvgPrivate' : 59,
                'CovidData' : TimeToDiagnosis.diagnosis(covid_data),
                'NotCovidData' : TimeToDiagnosis.diagnosis(not_covid_data),
                'AllData' : TimeToDiagnosis.diagnosis(data_df)
            
        }

        final_output['Diagnosis'] = Diagnosis
        final_output["DataDictionary"] = DataDictionarydictRecords


        AnnualAverages = {


                'CovidData' : TimeToDiagnosis.AnnualAverage(covid_data),
                'NotCovidData' : TimeToDiagnosis.AnnualAverage(not_covid_data),
                'AllData' : TimeToDiagnosis.AnnualAverage(data_df)
    
        }

        final_output['AnnualAverage'] = AnnualAverages

        CovidComparison = [

            {'Data': [round(covid_data['MRIReportDuration'].mean(),2),round(not_covid_data['MRIReportDuration'].mean(),2)], 'Label': 'MRI'},
            {'Data': [round(covid_data['ConsulationDuration'].mean(),2),round(not_covid_data['ConsulationDuration'].mean(),2)], 'Label': 'Consultant Appointment'},
            {'Data': [round(covid_data['BiopsyDuration'].mean(),2),round(not_covid_data['MRIReportDuration'].mean(),2)], 'Label': 'Biopsy Procedure'},
            {'Data': [round(covid_data['MRIToBiopsyDuration'].mean(),2),round(not_covid_data['MRIReportDuration'].mean(),2)], 'Label': 'Biopsy Booked'},
            {'Data': [round(covid_data['BiopsyReviewDuration'].mean(),2),round(not_covid_data['MRIReportDuration'].mean(),2)], 'Label': 'Biopsy Review'}
        ]


        final_output['CovidComparison'] = CovidComparison
        return final_output
