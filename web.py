
from datetime import date


from parser import results_to_dash_table, convert_response
from dash import Dash, dash_table, html, dcc, Output, State, Input

from searcher import Searcher
from utils import date_range


class DashApp:
    def __init__(self):
        self.sc = Searcher()
        self.dash_app = Dash(__name__)

        self.dash_app.layout = html.Div([
            dcc.Input(id='origins', type='text', value='BOS,NYC',placeholder='Origin IATA code'),

            dcc.Input(id='destinations', type='text', value='HKG,PVG',placeholder='Destination IATA code, support comma separated multiple destinations'),
            dcc.DatePickerRange(id='dates',min_date_allowed=date.today(),
                                initial_visible_month=date.today(),),
            html.Button('Search', id='search', n_clicks=0),
            dash_table.DataTable(id='datatable-interactivity',
                                data = results_to_dash_table([]),
                                style_data={
                                        'whiteSpace': 'pre-line',
                                        'height': 'auto',
                                            },
                                editable=True,
                                sort_action="native", # TODO server end sort needed for some columns e.g., price and duration
                                )
            ])

        self.dash_app.callback(Output('datatable-interactivity', 'data'),
                                Input('search', 'n_clicks'),
                                State('origins', 'value'),
                                State('destinations', 'value'),
                                State('dates', 'start_date'),
                                State('dates', 'end_date'),
                                prevent_initial_call=True,)(self.update_table)
    def update_table(self, n_clicks, origins, destinations, start_date, end_date):
        if n_clicks == 0:
            return results_to_dash_table([])
        origins = [''.join(ori.split()) for ori in origins.split(',')]
        destinations = [''.join(des.split()) for des in destinations.split(',')]
        dates = date_range(start_date, end_date)
        results = []
        for ori in origins:
            for des in destinations:
                for date in dates:
                    response = self.sc.search_for(ori, des, date)
                    v1 = convert_response(response)
                    results.extend(v1)
        return results_to_dash_table(results)