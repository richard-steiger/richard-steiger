import requests
import time
from datetime import datetime, timedelta
import sys
import json


class dater(object):
    def __init__(self, first, lastPlusOne, inclusiveEnd=False):
        # Store important stuff, adjusting end if you want it inclusive.

        self.__oneDay = timedelta(days=1)
        self.__curr = datetime.strptime(first, "%Y-%m-%d").date()
        self.__term = datetime.strptime(lastPlusOne, "%Y-%m-%d").date()
        if inclusiveEnd:
            self.__term += self.__oneDay

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        # This is the meat. It checks to see if the generator is
        # exhausted and raises the correct exception if so. If not,
        # it saves the current, calculates the next, stores that
        # for the next time, then returns the saved current.

        if self.__curr >= self.__term:
            raise StopIteration()

        (cur, self.__curr) = (self.__curr, self.__curr + self.__oneDay)
        return cur


def trivago_generator(env, reportName, type, backfillStartDate, backfillEndDate, auth_token):
    if env != 'test' and env != 'prod':
        sys.exit('Parameter env has to be either test or prod. It was ' + env)

    if type != 'daily' and type != 'backfill':
        sys.exit('Parameter type has to be either daily or backfill. It was ' + type)

    if type == 'backfill':
        if backfillStartDate == '':
            sys.exit('Parameter backfillStartDate has to be populated for backfill. It was empty')
        if backfillEndDate == '':
            sys.exit('Parameter backfillEndDate has to be populated for backfill. It was empty')
        if backfillEndDate < backfillStartDate:
            sys.exit('Parameter backfillEndDate has to be >= backfillStartDate for backfill.')

    if type == 'daily':
        if backfillStartDate != '':
            sys.exit('Parameter backfillStartDate has to be empty for daily. It was ' + backfillStartDate)
        if backfillEndDate != '':
            sys.exit('Parameter backfillEndDate has to be empty for daily. It was ' + backfillEndDate)

    if env == 'prod':
        fireflyUrl = 'http://pegasus-job-manager-service-v1-egdp-prod.us-east-1-vpc-018bd5207b3335f70.slb.egdp-prod.aws.away.black'   #### This is v1 swagger endpoint
        # fireflyUrl = 'http://pegasus-job-manager-service-egdp-prod.us-east-1-vpc-018bd5207b3335f70.slb.egdp-prod.aws.away.black'        #### This is v0 swagger endpoint
    else:
        fireflyUrl = 'http://pegasus-job-manager-service-v1-egdp-test.us-east-1-vpc-0618333437f727d62.slb.egdp-test.aws.away.black'   #### This is v1 swagger endpoint
        # fireflyUrl = 'http://pegasus-job-manager-service-egdp-test.us-east-1-vpc-0618333437f727d62.slb.egdp-test.aws.away.black'        #### This is v0 swagger endpoint

    partner = 'TRIVAGO'

    MINUTES_IN_DAY = 1440

    PARTNER = '{{PARTNER}}'
    BRAND = '{{BRAND}}'
    ACCOUNTID = '{{ACCOUNTID}}'
    DATE = '{{DATE}}'
    DATENODASH = '{{DATENODASH}}'
    REPORT = '{{REPORT}}'
    JOBNAME = '{{JOBNAME}}'
    JOBNAMEPREFIX = '{{JOBNAMEPREFIX}}'
    CRED = '{{CRED}}'
    USERNAME = '{{USERNAME}}'
    REPORTSCHEMA = '{{REPORTSCHEMA}}'
    TABLENAME = '{{TABLENAME}}'
    SELECTEXPR = '{{SELECTEXPR}}'
    REPORTFILENAME = '{{REPORTFILENAME}}'
    TIMEFRAME = '{{TIMEFRAME}}'
    VERSION = '"version":1'
    DATESTR = '${date.eval(America/Los_Angeles, yyyy-MM-dd, -1440)}'
    DATESTRNODASH = '${date.eval(America/Los_Angeles, yyyyMMdd, -1440)}'

    WORKFLOWNAME = '{{WORKFLOWNAME}}'
    WORKFLOWCRON = '{{WORKFLOWCRON}}'
    WORKFLOWEMAIL = '{{WORKFLOWEMAIL}}'
    WORKFLOWJOBS = '{{WORKFLOWJOBS}}'

    headers = {'Authorization': auth_token}

    with open('report/' + reportName + '.json') as json_file:
        reportDict = json.load(json_file)

    with open('vars/trivago_brand_vars.json') as json_file:
        brandsDict = json.load(json_file)

    with open("template/trivago_job_template.json", "r") as f:
        jobTemplate = f.read()

    print('--------jobTemplate----------')
    print(jobTemplate)
    print('----------------------')

    for report, reportVars in reportDict.items():
        list_items = []
        if type == 'backfill':
            for item in dater(backfillStartDate, backfillEndDate, inclusiveEnd=True):
                list_items.append(str(item))

            # For backfill start from end date back to start date
            list_items.reverse()
        else:
            windowStart = reportVars['paddingDays'] * -1
            windowEnd = windowStart - reportVars['windowDays']

            for item in range(windowStart, windowEnd, -1):
                list_items.append(item)

        workflowJobJsonStr = ''

        # Generate jobs for only applicable brands for this report
        for brandName in reportVars['reportBrands']:
            brandVars = brandsDict[brandName]

            jobNamePrefix = (partner + '-' + brandName + '-' + report + '-gen').replace('_', '-').lower()
            jobName = jobNamePrefix

            jobJsonDay = jobTemplate.replace(BRAND, brandVars['marketingBrand'])
            jobJsonDay = jobJsonDay.replace(REPORT, report)
            jobJsonDay = jobJsonDay.replace(PARTNER, partner)
            jobJsonDay = jobJsonDay.replace(ACCOUNTID, brandVars['accountId'])
            jobJsonDay = jobJsonDay.replace(USERNAME, brandVars['user'])
            jobJsonDay = jobJsonDay.replace(CRED, brandVars['cred'].lower())
            jobJsonDay = jobJsonDay.replace(JOBNAME, jobName)
            jobJsonDay = jobJsonDay.replace(REPORTFILENAME, reportVars['reportFileName'])
            jobJsonDay = jobJsonDay.replace(REPORTSCHEMA, reportVars['reportSchema'])
            jobJsonDay = jobJsonDay.replace(TABLENAME, reportVars['reportTableName'])
            jobJsonDay = jobJsonDay.replace(SELECTEXPR, reportVars['reportSelectExpr'])

            print('--------final---------')
            print(jobJsonDay)
            print('----------------------')
            response = requests.post(
                fireflyUrl + '/job-catalog/job',
                data=jobJsonDay,
                headers=headers)
            print("Status code: ", response.status_code)
            print('----------------------')
            print(response.json())
            time.sleep(.1)

            # Version 1 does not exist. Try changing to 0
            if response.status_code == 400:
                jobJsonDay = jobJsonDay.replace(VERSION, '"version":0')
                print('--------modified final---------')
                print(jobJsonDay)
                print('----------------------')
                response = requests.post(
                    fireflyUrl + '/job-catalog/job',
                    data=jobJsonDay,
                    headers=headers)
                print("Status code: ", response.status_code)
                print('----------------------')
                print(response.json())
                time.sleep(.1)

            if response.status_code != 200:
                print("ERROR posting Job. Existing application")
                sys.exit()


        with open("template/trivago_workflow_job_template.json", "r") as f:
            workflowJobJsonTemplate = f.read()

        with open("template/trivago_workflow_template.json", "r") as f:
            workflowTemplate = f.read()

            for brandName in reportVars['reportBrands']:
                brandVars = brandsDict[brandName]

                jobNamePrefix = (partner  + '-' + brandName + '-' + report + '-gen').replace('_', '-').lower()

                for item in list_items:
                    if type == 'backfill':
                        dateStr = str(item)
                        dateStrNoDash = dateStr.replace('-', '')
                        jobName = jobNamePrefix + "-" + dateStrNoDash
                    else:
                        dateStr = DATESTR.replace("-1440", str(item * MINUTES_IN_DAY))
                        dateStrNoDash = DATESTRNODASH.replace("-1440", str(item * MINUTES_IN_DAY))
                        jobName = jobNamePrefix + str(item)

                    workflowJobJsonStr = workflowJobJsonStr.replace('{{CHILDJOBJSON}}', jobName)
                    workflowJobJson = workflowJobJsonTemplate.replace(JOBNAME, jobName) \
                        .replace(JOBNAMEPREFIX, jobNamePrefix) \
                        .replace('{{DATE}}',dateStr)\
                        .replace('{{DATENODASH}}',dateStrNoDash)

                    workflowJobJsonStr = workflowJobJsonStr + '\n' + workflowJobJson + ','

                workflowJobJsonStr = workflowJobJsonStr.replace('"{{CHILDJOBJSON}}"', '')

        workflowJobJsonStr = workflowJobJsonStr[:-1]  # strip last comma


        print('--------workflowTemplate----------')
        print(workflowTemplate)
        print('----------------------')

        if type == 'backfill':
            workflowName = (reportVars['workflowNamePrefix'] + '-' + partner + '-' + report + '-backfill-gen').replace('_', '-').lower()
        else:
            workflowName = (reportVars['workflowNamePrefix'] + '-' + partner + '-' + report + '-download-gen').replace('_', '-').lower()

        workflowJson = workflowTemplate.replace(WORKFLOWNAME, workflowName)
        workflowJson = workflowJson.replace(WORKFLOWCRON, reportVars['workflowCron'])
        workflowJson = workflowJson.replace(WORKFLOWEMAIL, reportVars['workflowEmail'])
        workflowJson = workflowJson.replace(WORKFLOWJOBS, workflowJobJsonStr)

        print('--------workflowJson----------')
        print(workflowJson)
        print('----------------------')

        response = requests.post(
            fireflyUrl + '/job-catalog/workflow',
            data=workflowJson,
            headers=headers)
        print("Status code: ", response.status_code)
        print('----------------------')
        print(response.json())
        time.sleep(.1)

        if response.status_code != 200:
            print("ERROR posting Workflow. Existing application")
            sys.exit()

