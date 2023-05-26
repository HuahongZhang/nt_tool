from datetime import timedelta
import time
import collections

from aa_searcher import Aa_Searcher
from ac_searcher import Ac_Searcher
from dl_searcher import Dl_Searcher
from dynamo import FlightQuery
from nt_models import CabinClass, AirBound
from nt_parser import convert_aa_response_to_models, convert_ac_response_to_models, \
    convert_dl_response_to_models


def match_query(air_bound: AirBound, q: FlightQuery):
    if q.max_stops is not None and air_bound.stops > q.max_stops:
        # print(f"EXCEED MAX STOPS LIMIT, STOPS: {air_bound.stops}")
        return False
    if q.max_duration and air_bound.excl_duration_in_all_in_seconds > timedelta(
            hours=int(q.max_duration)):
        # print(f"EXCEED MAX DURATION LIMIT, DURATION: {air_bound.excl_duration_in_all_in_seconds}")
        return False
    if q.exact_airport and not (air_bound.from_to.startswith(q.origin) and
                                air_bound.from_to.endswith(q.destination)):
        # print(f"VIOLATE EXACT AIRPORT, AIRPORTS: {air_bound.from_to}")
        return False

    for price in air_bound.price:
        # print(f"engine: {air_bound.engine.upper()}, cabin_class: {price.cabin_class}, quota: {price.quota}, "
        #       f"price: {price.excl_miles}")
        if q.cabin_class and price.cabin_class != CabinClass.from_string(q.cabin_class):
            continue
        if q.num_passengers and price.quota < q.num_passengers:
            continue
        if q.cabin_class and CabinClass.from_string(q.cabin_class) != CabinClass.ECO and price.is_mix:
            mixes = price.mix_detail.split('+')
            eco_mix_sum = 0
            for mix in mixes:
                if 'Y' in mix:
                    eco_mix_sum += float(mix.strip('Y').strip('%'))
            if eco_mix_sum > 50:
                print('ECONOMY IN MIX EXCEED 50%')
                continue
        if air_bound.engine.upper() == "AA" and price.excl_miles <= q.max_aa_points:
            return True
        if air_bound.engine.upper() == "AC" and price.excl_miles <= q.max_ac_points:
            return True
        if air_bound.engine.upper() == "DL" and price.excl_miles <= q.max_dl_points:
            return True

    # Did not find any price in selected cabin.
    return False


def update_last_run_time(flight_queries_table, q):
    flight_queries_table.update_item(
        Key={
            "id": q.id
        },
        UpdateExpression="SET last_run = :last_run",
        ExpressionAttributeValues={
            ":last_run": int(time.time())
        }
    )


def send_notification(air_bounds: list, q: FlightQuery, ses_client):
    air_bounds_list = [str(x) for air_bound in air_bounds for x in air_bound.to_flatted_list()]
    if not air_bounds_list:
        return

    resp = ses_client.list_identities(IdentityType='EmailAddress')
    if not resp.get("Identities"):
        raise Exception("Cannot send notification because no ses verified identity exists")
    source_email = resp["Identities"][0]

    print(f"Sending email for {q.origin}-{q.destination} on {q.date}")
    message = f"Found {len(air_bounds_list)} flights, the first 10 flights are: \n"
    message += "\n".join(air_bounds_list[:10])
    ses_client.send_email(
        Source=source_email,
        Destination={
            'ToAddresses': [
                q.email,
            ],
        },
        Message={
            'Subject': {
                'Data': f'Reward Ticket Found for {q.origin}-{q.destination} on {q.date}'
            },
            'Body': {
                'Text': {
                    'Data': message
                }
            }
        }
    )


def send_summary(summary_dict: collections.defaultdict, q: FlightQuery, ses_client):
    resp = ses_client.list_identities(IdentityType='EmailAddress')
    if not resp.get("Identities"):
        raise Exception("Cannot send notification because no ses verified identity exists")
    source_email = resp["Identities"][0]

    print(f"Sending summary email")
    summary_str = summary_dict_to_str(summary_dict)
    status = 'Normal' if (summary_dict['AA'] * summary_dict['AC'] * summary_dict['DL']) > 0 else 'ERROR'
    ses_client.send_email(
        Source=source_email,
        Destination={
            'ToAddresses': [
                q.email,
            ],
        },
        Message={
            'Subject': {
                'Data': f'[{status}] Reward Ticket Summary for {q.origin}-{q.destination} on {q.date}'
            },
            'Body': {
                'Text': {
                    'Data': summary_str
                }
            }
        }
    )

def find_air_bounds(aas: Aa_Searcher, acs: Ac_Searcher, dls: Dl_Searcher, q: FlightQuery, ses_client=None):
    print(f'Search for {q}')

    air_bounds = []

    # Search from AA.
    if q.max_aa_points and q.max_aa_points > 0:
        response = aas.search_for(q.origin, q.destination, q.date)
        aa_air_bounds = convert_aa_response_to_models(response)
        air_bounds.extend(aa_air_bounds)
        print("Find AA flights: %d" % len(aa_air_bounds))

    # Search from AC.
    if q.max_ac_points and q.max_ac_points > 0:
        response = acs.search_for(q.origin, q.destination, q.date)
        ac_air_bounds = convert_ac_response_to_models(response)
        air_bounds.extend(ac_air_bounds)
        print("Find AC flights: %d" % len(ac_air_bounds))
        # print(ac_air_bounds)

    # Search from DL.
    if q.max_dl_points and q.max_dl_points > 0:
        response = dls.search_for(q.origin, q.destination, q.date)
        dl_air_bounds = convert_dl_response_to_models(response)
        air_bounds.extend(dl_air_bounds)
        print("Find DL flights: %d" % len(dl_air_bounds))

    # Process each result and send notification if found a match.
    for air_bound in air_bounds:
        if match_query(air_bound, q):
            yield air_bound


def summarize_air_bounds(air_bounds: list, summary: collections.defaultdict):
    for i, air_bound in enumerate(air_bounds):
        summary[air_bound.engine.upper()] += 1
        for price in air_bound.price:
            summary[price.cabin_class] += 1
            summary[f"{price.cabin_class} {price.mix_detail} {price.miles}"] += 1


def summary_dict_to_str(summary: collections.defaultdict):
    summary_str = f"AA: {summary['AA']}, AC: {summary['AC']}, DL: {summary['DL']}\n" \
          f"Y: {summary['Y']}, W: {summary['W']}, J: {summary['J']}, F: {summary['F']}\n" + \
          f"Summary:\n\t" + \
          '\n\t'.join(sorted([x for x in summary.keys() if x.endswith('k')]))
    return summary_str
