import os
import csv
import json
import logging
import pandas as pd
import pendulum
import itertools
import pprint as pp
from io import StringIO

from google.cloud import bigquery
from fazzfinancial.common.utils import override_filepath
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.cloud import secretmanager

import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from jinja2 import Template

from fazzfinancial.common.utils.get_gcp_credentials import get_credentials_from_secret_manager

GCP_PROJECT_ID = "d291209"
GCP_CONN_ID = "composer_worker"
airflow_environment = os.getenv("AIRFLOW_ENVIRONMENT")
local_tz = pendulum.timezone("Asia/Singapore")


def get_bq_data(sql_file_name):
    # Get the dynamic balance_value for each date (only daily report needs)
    hook = GoogleBaseHook(
        gcp_conn_id=GCP_CONN_ID,
    )
    credentials = hook.get_credentials()
    client = bigquery.Client(credentials=credentials)
    logging.info("Successfully created BigQuery client.")

    query_path = override_filepath(
        f"plugins/fazzfinancial/tasks/cimb_report_generation/sql/{sql_file_name}.sql"
    )
    query = open(query_path, "r", encoding="utf-8").read()
    print(query)

    data = client.query(query).to_dataframe()
    return data


### The first four functions:
###   - daily_csv_writer, periodic_csv_writer, generate_daily_cimb_report, generate_periodic_cimb_report
### were maintained by Allen. This is to follow the format that finance team provided us.


def daily_csv_writer(col1, col2, col3, col4):
    # Create an in-memory buffer
    logging.info("Writing data to csv...")
    buffer = StringIO()

    # Create a CSV writer that writes to the buffer
    writer = csv.writer(buffer)

    # Write the data to the buffer
    for c1, c2, c3, c4 in itertools.zip_longest(col1, col2, col3, col4):
        row = [c1, c2, c3, c4]
        writer.writerow(row)

    # Move the buffer cursor to the beginning
    buffer.seek(0)

    # Read the buffer contents into a dataframe
    csv_data_df = pd.read_csv(buffer)
    column_indices = [2, 3]
    new_names = ["", ""]
    old_names = csv_data_df.columns[column_indices]
    csv_data_df.rename(columns=dict(zip(old_names, new_names)), inplace=True)
    csv_data = csv_data_df.to_csv(index=False)
    return csv_data


def periodic_csv_writer(row1, row2, row3, row4, row5, row6, row7, row8, row9, row10, row11):
    logging.info("Writing data to csv...")
    buffer = StringIO()

    # Create a CSV writer that writes to the buffer
    writer = csv.writer(buffer)
    for i in range(1, 12):
        writer.writerow(eval("row{}".format(i)))

    # Move the buffer cursor to the beginning
    buffer.seek(0)

    # Read the buffer contents into a dataframe
    csv_data_df = pd.read_csv(buffer, index_col=None, header=None, skiprows=1)
    csv_data_df.columns = [
        "",
        "Logical",
        "",
        "",
        "",
        "",
        "",
        "",
        "BankRow",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
    ]
    csv_data = csv_data_df.to_csv(index=False)
    return csv_data


def generate_daily_cimb_report(report_date, **kwargs):
    """Generates a daily CIMB report in CSV format based on the provided DataFrame and report_date.

    Args:
        report_date (dict): A dictionary containing the report date information.

    Returns:
        None

    Writes:
        Creates a CSV file with the generated CIMB report.

    Raises:
        KeyError: If the required columns are not found in the DataFrame.
        FileNotFoundError: If there is an issue while creating or writing to the CSV file.
    """
    report_date = report_date.to_date_string()
    # bq_data = get_daily_data()

    sql_file_name = "report_eod_balance"
    bq_data = get_bq_data(sql_file_name)
    logging.info(
        "Successfully get data from fazzbiz_sg_datamart.reconciliation_report_eod_balance_report_numbers"
    )

    bq_data["period_date"] = pd.to_datetime(
        bq_data["period_date"]
    )  # chage data type from dbdate to datetime
    df = bq_data[bq_data["period_date"] == report_date]
    logging.info(f"Getting data from BQ completed for {report_date}")

    col1 = [
        # Title
        "Date",
        "Report Name:",
        "",
        "Contact:",
        "",
        # Logical
        "Logical",
        "SVF Balance",
        "Transaction Fee",
        "Interest",
        "Bank Charge",
        "Ops",
        "non-SVF Balance",
        "Total",
        "",
        # Physical
        "BankRow",
        "UOB",
        "CIMB SVF (2000593611)",
        "CIMB SVF (2000593623)",
        "CIMB SVF (2000700439)",
        "CIMB SVF (2000773148)",
        "Total",
        "",
        # Discerepancy
        "Discerepancy",
    ]
    col2 = [report_date, "Quick-Check Total SVF Report", "", "finance@xfers.com"]

    logical_rule = (
        "df.loc[(df['type'] == 'logical') & (df['property'] == '{}'), 'balance_value'].values[0]"
    )
    physical_rule = (
        "df.loc[(df['type'] == 'physical') & (df['property'] == '{}'), 'balance_value'].values[0]"
    )
    discrepancy_rule = (
        "df.loc[(df['type'] == 'check') & (df['property'] == '{}'), 'balance_value'].values[0]"
    )
    col3 = [
        # Title
        "",
        "",
        "",
        "",
        "",
        # Logical
        "Balance",
        eval(logical_rule.format("svf_balance")),
        eval(logical_rule.format("fee_account_ids")),
        eval(logical_rule.format("interest_account_ids")),
        eval(logical_rule.format("expense_account_ids")),
        eval(logical_rule.format("ops_total")),
        eval(logical_rule.format("non_svf_balance")),
        eval(logical_rule.format("total")),
        "",
        # Physical
        "Balance",
        eval(physical_rule.format("uob")),
        eval(physical_rule.format("cimb_3611")),
        eval(physical_rule.format("cimb_3623")),
        eval(physical_rule.format("cimb_0439")),
        eval(physical_rule.format("cimb_3148")),
        eval(physical_rule.format("total")),
        "",
        # Discerepancy
        eval(discrepancy_rule.format("check_by_total")),
    ]

    actual_cimb_rule = "'v' if df.loc[(df['type'] == 'check') & (df['property'] == '{}'), 'balance_value'].values[0] == 0 else ''"
    col4 = [
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "Check",
        "",
        eval(actual_cimb_rule.format("check_by_actual_cimb_3611")),
        eval(actual_cimb_rule.format("check_by_actual_cimb_3623")),
        eval(actual_cimb_rule.format("check_by_actual_cimb_0439")),
        eval(actual_cimb_rule.format("check_by_actual_cimb_3148")),
    ]

    result_csv = daily_csv_writer(col1, col2, col3, col4)
    logging.info("Write daily csv completed")

    ti = kwargs["ti"]
    ti.xcom_push(key="daily_csv", value=result_csv)  # store data into xcom


def generate_periodic_cimb_report(report_date, **kwargs):
    """Generates a periodic CIMB report in CSV format based on the provided DataFrame and report_date.

    Args:
        df (pd.DataFrame): The DataFrame containing the data for the report.
        report_date (dict): A dictionary containing the report date information.

    Returns:
        None

    Writes:
        Creates a CSV file with the generated CIMB report.

    Raises:
        KeyError: If the required columns are not found in the DataFrame.
        FileNotFoundError: If there is an issue while creating or writing to the CSV file.
    """

    last_month = report_date.subtract(months=1)
    query_date = last_month.format("YYYY-MM")
    start_of_period = last_month.start_of("month").to_date_string()
    end_of_period = last_month.end_of("month").to_date_string()

    report_date = {
        "query_date": query_date,
        "start_of_period": start_of_period,
        "end_of_period": end_of_period,
    }

    sql_file_name = "report_periodic_movement"
    bq_data = get_bq_data(sql_file_name)
    logging.info(
        "Successfully get data from fazzbiz_sg_datamart.reconciliation_report_periodic_movement_report_numbers"
    )

    bq_data["period_date"] = pd.to_datetime(
        bq_data["period_date"]
    )  # chage data type from dbdate to datetime
    df = bq_data[bq_data["period_date"] == report_date["query_date"]]
    logging.info(f'Getting data from BQ completed for {report_date["query_date"]}')

    row1 = ["", "Logical", "", "", "", "", "", "", "BankRow"]

    row2 = [
        # Title
        "",
        # Logical
        "SVF Balance",
        "non-SVF Balance",
        "Transaction Fee",
        "Interest",
        "Bank Charge",
        "Ops",
        "Logical Total",
        # Physical
        "UOB",
        "CIMB SVF (2000593611)",
        "CIMB SVF (2000593623)",
        "CIMB SVF (2000700439)",
        "CIMB SVF (2000763533)",
        "CIMB SVF (2000773148)",
        "CIMB SVF (2000763545)",
        "BankRow Total",
        # Reconciling Items
        "Reconciling Items",
    ]

    reconciling_rules = "df.loc[(df['type'] == 'check') & (df['category'] == '{}') & (df['property'] == '{}'), 'movement_value'].values[0]"
    start_rules = "df.loc[(df['type'] == '{}') & (df['category'] == 'start') & (df['property'] == '{}'), 'movement_value'].values[0]"
    row3 = [
        # Title
        "START OF PERIOD: " + report_date["start_of_period"],
        # Logical
        eval(start_rules.format("logical", "svf_balance")),
        eval(start_rules.format("logical", "non_svf_balance")),
        eval(start_rules.format("logical", "fee_account_ids")),
        eval(start_rules.format("logical", "interest_account_ids")),
        eval(start_rules.format("logical", "expense_account_ids")),
        eval(start_rules.format("logical", "ops_total")),
        eval(start_rules.format("logical", "total")),
        # Physical
        eval(start_rules.format("physical", "uob")),
        eval(start_rules.format("physical", "cimb_3611")),
        eval(start_rules.format("physical", "cimb_3623")),
        eval(start_rules.format("physical", "cimb_0439")),
        eval(start_rules.format("physical", "cimb_3533")),
        eval(start_rules.format("physical", "cimb_3148")),
        eval(start_rules.format("physical", "cimb_3545")),
        eval(start_rules.format("physical", "total")),
        # Reconciling Items
        eval(reconciling_rules.format("check_by_row", "check_start")),
    ]

    row4 = []

    inflow_rules = "df.loc[(df['type'] == '{}') & (df['category'] == 'inflow') & (df['property'] == '{}'), 'movement_value'].values[0]"
    row5 = [
        # Title
        "inflow",
        # Logical
        eval(inflow_rules.format("logical", "inflow_svf_movement")),
        "",
        "",
        eval(inflow_rules.format("logical", "inflow_interest_movement")),
        "",
        eval(inflow_rules.format("logical", "inflow_ops_movement")),
        eval(inflow_rules.format("logical", "total")),
        # Physical
        eval(inflow_rules.format("physical", "inflow_uob")),
        eval(inflow_rules.format("physical", "inflow_cimb_3611")),
        eval(inflow_rules.format("physical", "inflow_cimb_3623")),
        eval(inflow_rules.format("physical", "inflow_cimb_0439")),
        eval(inflow_rules.format("physical", "inflow_cimb_3533")),
        eval(inflow_rules.format("physical", "inflow_cimb_3148")),
        eval(inflow_rules.format("physical", "inflow_cimb_3545")),
        eval(inflow_rules.format("physical", "total")),
        # Reconciling Items
        eval(reconciling_rules.format("check_by_row", "check_inflow")),
    ]

    outflow_rules = "df.loc[(df['type'] == '{}') & (df['category'] == 'outflow') & (df['property'] == '{}'), 'movement_value'].values[0]"
    row6 = [
        # Title
        "outflow",
        # Logical
        eval(outflow_rules.format("logical", "outflow_svf_movement")),
        "",
        eval(outflow_rules.format("logical", "outflow_transaction_fee_movement")),
        eval(outflow_rules.format("logical", "outflow_interest_movement")),
        eval(outflow_rules.format("logical", "outflow_expense_movement")),
        eval(outflow_rules.format("logical", "outflow_ops_movement")),
        eval(outflow_rules.format("logical", "total")),
        # Physical
        eval(outflow_rules.format("physical", "outflow_uob")),
        eval(outflow_rules.format("physical", "outflow_cimb_3611")),
        eval(outflow_rules.format("physical", "outflow_cimb_3623")),
        eval(outflow_rules.format("physical", "outflow_cimb_0439")),
        eval(outflow_rules.format("physical", "outflow_cimb_3533")),
        eval(outflow_rules.format("physical", "outflow_cimb_3148")),
        eval(outflow_rules.format("physical", "outflow_cimb_3545")),
        eval(outflow_rules.format("physical", "total")),
        # Reconciling Items
        eval(reconciling_rules.format("check_by_row", "check_outflow")),
    ]

    internal_rules = "df.loc[(df['type'] == '{}') & (df['category'] == 'internal') & (df['property'] == '{}'), 'movement_value'].values[0]"
    row7 = [
        # Title
        "internal",
        # Logical
        eval(internal_rules.format("logical", "internal_total_svf_movement")),
        eval(internal_rules.format("logical", "internal_non_svf_balance")),
        eval(internal_rules.format("logical", "internal_total_transaction_fee_movement")),
        eval(internal_rules.format("logical", "internal_interest_movement")),
        eval(internal_rules.format("logical", "internal_expense_movement")),
        eval(internal_rules.format("logical", "internal_total_ops_movement")),
        eval(internal_rules.format("logical", "total")),
    ]

    row8 = []

    end_rules = "df.loc[(df['type'] == '{}') & (df['category'] == 'end') & (df['property'] == '{}'), 'movement_value'].values[0]"
    row9 = [
        # Title
        "END OF PERIOD: " + report_date["end_of_period"],
        # Logical
        eval(end_rules.format("logical", "svf_balance")),
        eval(end_rules.format("logical", "non_svf_balance")),
        eval(end_rules.format("logical", "fee_account_ids")),
        eval(end_rules.format("logical", "interest_account_ids")),
        eval(end_rules.format("logical", "expense_account_ids")),
        eval(end_rules.format("logical", "ops_total")),
        eval(end_rules.format("logical", "total")),
        # Physical
        eval(end_rules.format("physical", "uob")),
        eval(end_rules.format("physical", "cimb_3611")),
        eval(end_rules.format("physical", "cimb_3623")),
        eval(end_rules.format("physical", "cimb_0439")),
        eval(end_rules.format("physical", "cimb_3533")),
        eval(end_rules.format("physical", "cimb_3148")),
        eval(end_rules.format("physical", "cimb_3545")),
        eval(end_rules.format("physical", "total")),
        # Reconciling Items
        eval(reconciling_rules.format("check_by_row", "check_end")),
    ]

    row10 = []

    row11 = [
        # Title
        "column check (start+inflow+outflow+internal-end)",
        # Logical
        eval(reconciling_rules.format("check_by_column", "logical_svf_movement")),
        eval(reconciling_rules.format("check_by_column", "logical_non_svf_balance")),
        eval(reconciling_rules.format("check_by_column", "logical_transaction_fee_movement")),
        eval(reconciling_rules.format("check_by_column", "logical_interest_movement")),
        eval(reconciling_rules.format("check_by_column", "logical_expense_movement")),
        eval(reconciling_rules.format("check_by_column", "logical_ops_movement")),
        eval(reconciling_rules.format("check_by_column", "logical_total")),
        # Physical
        eval(reconciling_rules.format("check_by_column", "physical_uob")),
        eval(reconciling_rules.format("check_by_column", "physical_cimb_3611")),
        eval(reconciling_rules.format("check_by_column", "physical_cimb_3623")),
        eval(reconciling_rules.format("check_by_column", "physical_cimb_0439")),
        eval(reconciling_rules.format("check_by_column", "physical_cimb_3533")),
        eval(reconciling_rules.format("check_by_column", "physical_cimb_3148")),
        eval(reconciling_rules.format("check_by_column", "physical_cimb_3545")),
        eval(reconciling_rules.format("check_by_column", "physical_total")),
    ]

    result_csv = periodic_csv_writer(
        row1, row2, row3, row4, row5, row6, row7, row8, row9, row10, row11
    )
    logging.info("Write periodic csv completed")

    ti = kwargs["ti"]
    ti.xcom_push(key="periodic_csv", value=result_csv)  # store data into xcom


def render_html_template(type):

    # Get the dynamic balance_value for each date (only daily report needs)
    hook = GoogleBaseHook(
        gcp_conn_id=GCP_CONN_ID,
    )
    credentials = hook.get_credentials()
    client = bigquery.Client(credentials=credentials)
    logging.info("Successfully created BigQuery client.")

    query_job = client.query(
        """
        SELECT balance_value 
        FROM d291209.fazzbiz_sg_datamart.reconciliation_report_eod_balance_report_numbers 
        WHERE type = "logical" 
        AND property = "svf_balance" 
        AND period_date = current_date - 1 
        """
    )

    results = query_job.result()
    for row in list(results):
        balance_val = row.balance_value

    logging.info(f"SVF Balance value is {balance_val}")

    # Separate different support_data for 2 reports
    support_data = {}
    if type == "periodic":
        support_data["start_date"] = (
            pendulum.now("Asia/Singapore")
            .subtract(months=1)
            .start_of("month")
            .format("DD MMM YYYY")
        )
        support_data["end_date"] = (
            pendulum.now("Asia/Singapore").subtract(months=1).end_of("month").format("DD MMM YYYY")
        )
        html_template_path = "mail_template/cimb_periodic_report_template.html"
    elif type == "daily":
        support_data["ytd"] = pendulum.yesterday("Asia/Singapore").format(
            "YYYY-MM-DD 00:00:00 +0800"
        )
        support_data["svf_balance"] = balance_val
        html_template_path = "mail_template/cimb_daily_report_template.html"

    dir_path = override_filepath("plugins/fazzfinancial/tasks/cimb_report_generation/")
    html_template = open(dir_path + html_template_path, "r").read()
    support_data["current_year"] = pendulum.now("Asia/Singapore").year

    logging.info(f"Support data for {type} report: ")
    pp.pprint(support_data)

    # Render the template with the dynamic value
    template = Template(html_template)
    rendered_html = template.render(support_data)

    return rendered_html


def send_email_smtp(csv_data, message, type, **kwargs):
    secret_name = "airflow__connections__sendgrid_api_key"
    credentials = json.loads(get_credentials_from_secret_manager(secret_name))

    # Prepare email recipients
    if airflow_environment in ["local", "testing", "development"]:
        recipients = [
            # "jay.liao@fazzfinancial.com",
            # "allen.chang@fazzfinancial.com",
            # "seongper@fazzfinancial.com",
            "louise.kuo@fazzfinancial.com",
        ]
        cc_recipients = ["louise.kuo@fazzfinancial.com"]
        bcc_recipients = []
    else:
        sql_file_name = "report_email_list"
        data = get_bq_data(sql_file_name)
        daily_recipients_to = data["user_email"][data["daily_report_type"] == "to"]
        daily_recipients_cc = data["user_email"][data["daily_report_type"] == "cc"]
        daily_recipients_bcc = data["user_email"][data["daily_report_type"] == "bcc"]
        periodic_recipients_to = data["user_email"][data["monthly_report_type"] == "to"]
        periodic_recipients_cc = data["user_email"][data["monthly_report_type"] == "cc"]
        periodic_recipients_bcc = data["user_email"][data["monthly_report_type"] == "bcc"]

    # Prepare the email message
    type = type.capitalize()
    report_date = pendulum.now(local_tz).date() - pendulum.duration(days=1)

    # Prepare some variables depend on report type
    if type == "Periodic":
        last_month = report_date.subtract(months=1)
        fdate = last_month.format("YYYY-MM")
        if airflow_environment in ["local", "testing", "development"]:
            RECIPIENT_LIST = ", ".join(recipients)
            CC_LIST = ", ".join(cc_recipients)
            BCC_LIST = ", ".join(bcc_recipients)
        else:
            RECIPIENT_LIST = ", ".join(periodic_recipients_to.to_list())
            CC_LIST = ", ".join(periodic_recipients_cc.to_list())
            BCC_LIST = ", ".join(periodic_recipients_bcc.to_list())
        SUBJECT = "[XFERS] Monthly SVF Reconciliation Report"
    elif type == "Daily":
        fdate = report_date
        if airflow_environment in ["local", "testing", "development"]:
            RECIPIENT_LIST = ", ".join(recipients)
            CC_LIST = ", ".join(cc_recipients)
            BCC_LIST = ", ".join(bcc_recipients)
        else:
            RECIPIENT_LIST = ", ".join(daily_recipients_to.to_list())
            CC_LIST = ", ".join(daily_recipients_cc.to_list())
            BCC_LIST = ", ".join(daily_recipients_bcc.to_list())
        SUBJECT = "[XFERS] Quick-Check Total SVF Report "

    filename = f"{fdate}_Cimb{type}Report.csv"  # daily or periodic
    subject = SUBJECT
    message = message  # email content will be rendered in html format

    msg = MIMEMultipart()
    msg["From"] = "Fazz Business" + " <no-reply@fazz.com>"
    msg["To"] = RECIPIENT_LIST
    msg["Cc"] = CC_LIST
    msg["Bcc"] = BCC_LIST
    msg["Subject"] = subject

    # Attach texts
    msgText = MIMEText(message, "html")
    msg.attach(msgText)

    # Attach content images
    image_data = {
        "cs": "image_cs",
        "LogoFazzBusiness": "image_logo_fazzbusiness",
        "computer": "image_computer",
        "FacebookBtn": "image_facebook",
        "InstagramBtn": "image_instagram",
        "TwitterBtn": "image_twitter",
        "LinkedinBtn": "image_linkedin",
    }
    for image_name, html_target in image_data.items():
        fp = open(
            override_filepath(
                f"plugins/fazzfinancial/tasks/cimb_report_generation/images/{image_name}.png"
            ),
            "rb",
        )
        msgImage = MIMEImage(fp.read())
        fp.close()

        # Define the image's ID as referenced above
        header_target = f"<{html_target}>"
        msgImage.add_header("Content-ID", header_target)
        msg.attach(msgImage)

    # Attach the CSV file
    part = MIMEBase("application", "octet-stream")
    part.set_payload(csv_data)
    part.add_header("Content-Disposition", f"attachment; filename={filename}")
    msg.attach(part)

    # Connect to SendGrid's SMTP server and send the email
    smtp_server = "smtp.sendgrid.net"
    smtp_port = 587
    username = credentials["username"]
    password = credentials["password"]

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.ehlo()
        server.starttls()
        server.login(username, password)
        server.send_message(msg)


def send_daily_report(ti, **kwargs):
    task_id = kwargs["task_id"]
    csv_data = ti.xcom_pull(key="daily_csv", task_ids=task_id)
    logging.info("Getting daily report csv...")

    message = render_html_template("daily")
    send_email_smtp(csv_data, message, "daily")
    logging.info("Sending daily report completed.")


def send_periodic_report(ti, **kwargs):
    task_id = kwargs["task_id"]
    csv_data = ti.xcom_pull(key="periodic_csv", task_ids=task_id)
    logging.info("Getting periodic report csv...")

    message = render_html_template("periodic")
    send_email_smtp(csv_data, message, "periodic")
    logging.info("Sending completed.")
