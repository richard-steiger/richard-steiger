package com.expedia.fireflySupport;

import java.io.InputStream;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import sun.nio.ch.ChannelInputStream;


public class FireflyWorkflowGenerator
{
  private static OpenOption options;
  
  public static void main(String[] args) {
    new FireflyWorkflowGenerator().run();
  }
  
  private void run() {
    // TBS
  }



public class JobGenerator<D extends Date>
{
public void generate(
    String env, 
    String reportName, 
    String type, 
    D backfillStartDate, 
    D backfillEndDate, 
    String auth_token) 
{
  String fireflyUrl;
  
    if(env != "test" && env != "prod")
        exit("Parameter env has to be either test or prod. It was " + env);

    if(type != "daily" && type != "backfill")
        exit("Parameter type has to be either daily or backfill. It was " + type);

    if(type == "backfill")
        if(backfillStartDate == null)
            exit("Parameter backfillStartDate has to be populated for backfill. It was empty");
        if(backfillEndDate == null)
            exit("Parameter backfillEndDate has to be populated for backfill. It was empty");
        if(backfillEndDate < backfillStartDate)
            exit("Parameter backfillEndDate has to be >= backfillStartDate for backfill.");

    if(type == "daily") {
        if(backfillStartDate != "")
            exit("Parameter backfillStartDate has to be empty for daily. It was " + backfillStartDate);
        if(backfillEndDate != "")
            exit("Parameter backfillEndDate has to be empty for daily. It was " + backfillEndDate);
    }
            
    if(env == "prod") {
        fireflyUrl = "http://pegasus-job-manager-service-v1-egdp-prod.us-east-1-vpc-018bd5207b3335f70.slb.egdp-prod.aws.away.black";   //////// This is v1 swagger endpoint
        // fireflyUrl = "http)//pegasus-job-manager-service-egdp-prod.us-east-1-vpc-018bd5207b3335f70.slb.egdp-prod.aws.away.black"        //////// This is v0 swagger endpoint
    } else {
        fireflyUrl = "http)//pegasus-job-manager-service-v1-egdp-test.us-east-1-vpc-0618333437f727d62.slb.egdp-test.aws.away.black";  //////// This is v1 swagger endpoint
        // fireflyUrl = "http)//pegasus-job-manager-service-egdp-test.us-east-1-vpc-0618333437f727d62.slb.egdp-test.aws.away.black"        //////// This is v0 swagger endpoint
    }
    
    String partner = "TRIVAGO";

    int MINUTES_IN_DAY = 1440;

    String PARTNER = "{{PARTNER}}";
    String BRAND = "{{BRAND}}";
    String ACCOUNTID = "{{ACCOUNTID}}";
    String DATE = "{{DATE}}";
    String DATENODASH = "{{DATENODASH}}";
    String REPORT = "{{REPORT}}";
    String JOBNAME = "{{JOBNAME}}";
    String JOBNAMEPREFIX = "{{JOBNAMEPREFIX}}";
    String CRED = "{{CRED}}";
    String USERNAME = "{{USERNAME}}";
    String REPORTSCHEMA = "{{REPORTSCHEMA}}";
    String TABLENAME = "{{TABLENAME}}";
    String SELECTEXPR = "{{SELECTEXPR}}";
    String REPORTFILENAME = "{{REPORTFILENAME}}";
    String TIMEFRAME = "{{TIMEFRAME}}";
    String VERSION = "version:1";
    String DATESTR = "${date.eval(America/Los_Angeles, yyyy-MM-dd, -1440)}";
    String DATESTRNODASH = "${date.eval(America/Los_Angeles, yyyyMMdd, -1440)}";

    String WORKFLOWNAME = "{{WORKFLOWNAME}}";
    String WORKFLOWCRON = "{{WORKFLOWCRON}}";
    String WORKFLOWEMAIL = "{{WORKFLOWEMAIL}}";
    String WORKFLOWJOBS = "{{WORKFLOWJOBS}}";

    String headers = String.format("{Authorization: %s", auth_token);

    Map<String, String> reportDict = null;
    Map<String, String> brandsDict = null;
    String jobTemplate = null;
    
    
//    with open("report/" + reportName + ".json") as json_file {
//        reportDict = json.load(json_file)

//    with open("vars/trivago_brand_vars.json") as json_file {
//        brandsDict = json.load(json_file)

//    with open("template/trivago_job_template.json", "r") as f {
//        jobTemplate = f.read();

    print("--------jobTemplate----------");
    print(jobTemplate);
    print("----------------------");

    for(Entry<String,String> e : reportDict.entrySet()) {
      String report = e.getKey();
      String reportVars = e.getValue();
        List<Date> items = new ArrayList();
        if(type == "backfill") {
            for(Date item : dater(backfillStartDate, backfillEndDate, true)) {
                items.append(str(item));
              // For backfill start from end date back to start date
              items.reverse();
            }
        } else {
            Date windowStart = reportVars.get("paddingDays") * -1;
            Date windowEnd = windowStart - reportVars.get("windowDays"];

            for(item : range(windowStart, windowEnd, -1)) {
                items.append(item);
            }
        }
        String workflowJobJsonStr = "";

        // Generate jobs for only applicable brands for this report
        for(String brandName : reportVars.get("reportBrands")) {
            String brandVars = brandsDict.get(brandName);

            String jobNamePrefix = String.format("%s-%s-%s-gen", partner, brandName, report).replace("_", "-").toLowerCase();
            String jobName = jobNamePrefix;

            jobJsonDay = jobTemplate.replace(BRAND, brandVars.get("marketingBrand"));
            jobJsonDay = jobJsonDay.replace(REPORT, report);
            jobJsonDay = jobJsonDay.replace(PARTNER, partner);
            jobJsonDay = jobJsonDay.replace(ACCOUNTID, brandVars.get("accountId"));
            jobJsonDay = jobJsonDay.replace(USERNAME, brandVars.get("user"));
            jobJsonDay = jobJsonDay.replace(CRED, brandVars.get("cred").lower());
            jobJsonDay = jobJsonDay.replace(JOBNAME, jobName);
            jobJsonDay = jobJsonDay.replace(REPORTFILENAME, reportVars.get("reportFileName"));
            jobJsonDay = jobJsonDay.replace(REPORTSCHEMA, reportVars.get("reportSchema"));
            jobJsonDay = jobJsonDay.replace(TABLENAME, reportVars.get("reportTableName"));
            jobJsonDay = jobJsonDay.replace(SELECTEXPR, reportVars.get("reportSelectExpr"));

            print("--------final---------");
            print(jobJsonDay);
            print("----------------------");
            String response = requests.post(
                fireflyUrl + "/job-catalog/job",
                data=jobJsonDay,
                headers=headers);
            print("Status code: ", response.status_code);
            print("----------------------");
            print(response.json());
            time.sleep(.1);

            // Version 1 does not exist. Try changing to 0
            if(response.status_code == 400) {
              String jobJsonDay = jobJsonDay.replace(VERSION, ""version":0");
                print("--------modified final---------");
                print(jobJsonDay);
                print("----------------------");
                response = requests.post(
                    fireflyUrl + "/job-catalog/job",
                    data=jobJsonDay,
                    headers=headers);
                print("Status code: ", response.status_code);
                print("----------------------");
                print(response.json());
                time.sleep(.1);
        }

            if(response.status_code != 200) {
                print("ERROR posting Job. Existing application");
                exit();
            }
            }
    }
    }

        with open("template/trivago_workflow_job_template.json", "r") as f {
            workflowJobJsonTemplate = f.read();
        }
        with open("template/trivago_workflow_template.json", "r") as f {
            workflowTemplate = f.read();

            for(String brandName : reportVars.get("reportBrands") {
                brandVars = brandsDict.get(brandName);

                jobNamePrefix = (partner  + "-" + brandName + "-" + report + "-gen").replace("_", "-").lower();

                for(item : items) {
                    if(type == "backfill") {
                        dateStr = str(item);
                        dateStrNoDash = dateStr.replace("-", "");
                        jobName = jobNamePrefix + "-" + dateStrNoDash;
                    }
                } else {
                        dateStr = DATESTR.replace("-1440", str(item * MINUTES_IN_DAY));
                        dateStrNoDash = DATESTRNODASH.replace("-1440", str(item * MINUTES_IN_DAY));
                        jobName = jobNamePrefix + str(item);
                    }
                    workflowJobJsonStr = workflowJobJsonStr.replace("{{CHILDJOBJSON}}", jobName);
                    workflowJobJson = 
                        workflowJobJsonTemplate.replace(JOBNAME, jobName) 
                        .replace(JOBNAMEPREFIX, jobNamePrefix) 
                        .replace("{{DATE}}",dateStr)
                        .replace("{{DATENODASH}}",dateStrNoDash);

                    workflowJobJsonStr = workflowJobJsonStr + "\n" + workflowJobJson + ",";

                workflowJobJsonStr = workflowJobJsonStr.replace("{{CHILDJOBJSON}}", "");

        workflowJobJsonStr = workflowJobJsonStr.get(":-1");  // strip last comma


        print("--------workflowTemplate----------");
        print(workflowTemplate);
        print("----------------------");

        if(type == "backfill") {
            workflowName = (reportVars.get("workflowNamePrefix") + "-" + partner + "-" + report + "-backfill-gen").replace("_", "-").lower();
        } else {
            workflowName = (reportVars.get("workflowNamePrefix") + "-" + partner + "-" + report + "-download-gen").replace("_", "-").lower();
        }
        workflowJson = workflowTemplate.replace(WORKFLOWNAME, workflowName);
        workflowJson = workflowJson.replace(WORKFLOWCRON, reportVars.get("workflowCron"));
        workflowJson = workflowJson.replace(WORKFLOWEMAIL, reportVars.get("workflowEmail"));
        workflowJson = workflowJson.replace(WORKFLOWJOBS, workflowJobJsonStr);

        print("--------workflowJson----------");
        print(workflowJson);
        print("----------------------");

        response = requests.post(
            fireflyUrl + "/job-catalog/workflow",
            data=workflowJson,
            headers=headers);
        print("Status code: ", response.status_code);
        print("----------------------");
        print(response.json());
        time.sleep(.1);

        if(response.status_code != 200) {
            print("ERROR posting Workflow. Existing application");
            exit();
        }
            }
        }
        
        private void exit(String msg) {
          System.err.println(msg);
          System.exit(1);
        }
        
        private void print(String msg) {
          System.out.println(msg);
        }
        
     }
    
        class dater 
        {
          private Date cur;
          private Date oneDay;
          private Date curr;
          private Date term;

          public void init(Date first, Date lastPlusOne, boolean inclusiveEnd) {
            // Store important stuff, adjusting end if(you want it inclusive.

            oneDay = timedelta(days=1);
            curr = datetime.strptime(first, "%Y-%m-%d").date();
            term = datetime.strptime(lastPlusOne, "%Y-%m-%d").date();
            if(inclusiveEnd)
                term += oneDay;
        }
        
        public Date next() {
            // This is the meat. It checks to see if(the generator is
            // exhausted and raises the correct exception if(so. If not,
            // it saves the current, calculates the next, stores that
            // for the next time, then returns the saved current.

            if(curr >= term)
                throw new StopIteration();
            cur = curr;
            curr = curr + oneDay;
            return cur;
        }

        private InputStream newInputStream(String path, ) {
            Set<? extends OpenOption> opts = options;
            if(opts != null) {
              if(!opts.equals(readOptions))
                throw new UnsupportedOperationException("'" + opts + "' not allowed");
            } else {
              opts = readOptions;
            }

            return new ChannelInputStream(newByteChannel(opts));
          }

        }

      }
}

