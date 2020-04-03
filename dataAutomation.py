import pandas as pd
import boto3
import io

# S3 configs
s3 = boto3.resource('s3')
bucket = s3.Bucket('student-grades') #bucket name

csvLists = []
data = []
result = []

# get s3 key for reports 
for key in bucket.objects.all():
    if 'grade_report' in key.key and 'problem_grade_report' not in key.key:
        csvLists.append(key.key)

for sheet in csvLists:

    obj = s3.Object('student-grades', sheet)

    data = obj.get()['Body'].read()

    data = unicode(data, "utf-8") #convert unicode data into utf-8 encoded data

    data = io.StringIO(data) #convert teh utf-8 coded data into a valid string

    data = pd.read_csv(data) # reading the data string using Pandas
    
    courseId = sheet.split('/')[1].split('_')

    data['Course ID'] = pd.Series(courseId[0] + '_' + courseId[1] + '_' + courseId[2], index = data.index) # get course IDs

    if ('Project (Avg)' in data.columns):
        data = data.rename(columns={'Project 2: Project':'Project Assessment'}) #renaming the column name Project 2: Project to a project assessment

    elif ('Project Assessment (Avg)' in data.columns):
        data = data.rename(columns={'Project Assessment 2: Activity 6.2 Final project':'Project Assessment'}) #renaming the column name Project Assessment 2: Activity 6.2 Final project to a project assessment

    elif ('Assessment Project' in data.columns):
        data = data.rename(columns={'Assessment Project':'Project Assessment'}) #renaming the column name Assessment Project to a project assessment

    else:
        data['Project Assessment'] = pd.Series(0, index = data.index)

    result.append(data[['Email', 'Student ID', 'Username', 'Course ID', 'Grade', 'Verification Status', 'Certificate Eligible', 'Certificate Delivered', 'Certificate Type', 'Project Assessment','Enrollment Track', 'Enrollment Status']])
    
result = pd.concat(result, axis=0, ignore_index=False) #concatenat each data to a singe csv
result.to_excel('Grade-report.xlsx', sheet_name='passengers', index=False) # generate a csv named MichaelData
