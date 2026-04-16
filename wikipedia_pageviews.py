"""
Airflow DAG: Wikipedia Pageviews Pipeline
- Downloads hourly Wikipedia pageview data from Wikimedia
- Parses and filters pageviews for specified pages
- Stores results to a local file (easily swappable for DB/GCS/S3)
"""
from urllib import request 
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator


def _get_data(year, month, day, hour, output_path, **_):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    print(f"Fetching URL: {url}")  # ← add this
    request.urlretrieve(url, output_path)

def _fetch_pageviews(pagenames, logical_date, **_):
    result = dict.fromkeys(pagenames, 0)
    with open (f"/tmp/wikipageviews-{ logical_date.format('YYYYMMDDHH') }") as f:
        for line in f:
            domain_code, page_title, view_counts, _ =line.split(" ")
            if domain_code =='en' and page_title in pagenames:
                result[page_title] = int(view_counts)

    print(result)
    # Print e.g. "{'Facebook' : '778', 'Apple':'20', 'Google' : '451', 'Amazon':'9', 'Microsoft':'119'}"

with DAG(
    dag_id = "wikipedia_pageviews", 
    start_date = pendulum.today("UTC").substract(days=1),
    schedule = "@hourly", 
    max_active_runs = 1,
    catchup = True,
    default_args = {
        "retires" : 3, 
        "retry_delay" : pendulum.duration(minutes = 20), 
    }
): 
    get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{ data_interval_start.year }}",
        "month": "{{ data_interval_start.month }}",
        "day": "{{ data_interval_start.day }}",
        "hour": "{{ data_interval_start.hour }}",
        "output_path": "/tmp/wikipageviews-{{ data_interval_start.format('YYYYMMDDHH') }}.gz",
    },
)
    extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command="""
        echo "Looking for file..."
        ls -lh /tmp/wikipageviews* || echo "NO FILES FOUND in /tmp"
        echo "Attempting gunzip..."
        gunzip --force /tmp/wikipageviews-{{ data_interval_start.format('YYYYMMDDHH') }}.gz
        echo "Done."
    """,
)

    fetch_pageviews = PythonOperator(
        task_id = "fetch_pageviews", 
        python_callable = _fetch_pageviews, 
        op_kwargs = {
            "pagenames": {
                "Google", 
                "Amazon",
                "Apple", 
                "Microsoft",
                "Facebook",
            }
        },
    )
    get_data >> extract_gz >> fetch_pageviews