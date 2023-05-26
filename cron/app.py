import boto3
import collections

from dynamo import fetch_all_queries_from_dynamo
from process import find_air_bounds, send_notification, send_summary, update_last_run_time, summarize_air_bounds
from aa_searcher import Aa_Searcher
from ac_searcher import Ac_Searcher
from dl_searcher import Dl_Searcher

LIMIT = 30
MIN_RUN_GAP = 4 * 3600
MIN_TEST_GAP = 24 * 3600

dynamodb = boto3.resource('dynamodb')
flight_queries_table = dynamodb.Table('flight_queries')
# uncomment below if you want a summary email to make sure the service is running
# flight_query_test_table = dynamodb.Table('flight_query_test')
ses_client = boto3.client('ses')

aas = Aa_Searcher()
acs = Ac_Searcher()
dls = Dl_Searcher()


def handler(event, context):
    for q in fetch_all_queries_from_dynamo(flight_queries_table, LIMIT, MIN_RUN_GAP):
        air_bounds = find_air_bounds(aas, acs, dls, q)
        send_notification(air_bounds, q, ses_client)
        update_last_run_time(flight_queries_table, q)

    # send summary email
    # for q in fetch_all_queries_from_dynamo(flight_query_test_table, LIMIT, MIN_TEST_GAP):
    #     summary_dict = collections.defaultdict(int)
    #     air_bounds = find_air_bounds(aas, acs, dls, q)
    #     summarize_air_bounds(air_bounds, summary_dict)
    #     send_summary(summary_dict, q, ses_client)
    #     update_last_run_time(flight_query_test_table, q)

    return "success"
