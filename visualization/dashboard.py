"""
Documentation used for deployment on EC2:
https://medium.com/@GeoffreyGordonAshbrook/plotly-dash-in-ec2-production-server-502717843efb
"""
import datetime as dt
import pandas as pd
import plotly.express as px
from dash import Dash
from dash import html
from dash import dash_table
from dash import callback
from dash import Output
from dash import Input
from dash import dcc

from weatherreport.data.helpers import get_all_city_names
from weatherreport.database.dbAPI import db_wrapper_factory

dblAPI = db_wrapper_factory(db_type="bigquery")

app = Dash(__name__)

server = app.server
# App layout
app.layout = html.Div(
    [
        html.Div(children="South Bay Weather"),
        dash_table.DataTable(id="temperature_table", data=[], page_size=10),
        dcc.Graph(id="max_temperature", figure={}),
        dcc.Interval(
            id="interval_component",
            interval=30 * 1000,  # in milliseconds
            n_intervals=0,
        ),
    ]
)


@callback(
    [
        Output(component_id="temperature_table", component_property="data"),
        Output(component_id="max_temperature", component_property="figure"),
    ],
    Input(component_id="interval_component", component_property="n_intervals"),
)
def update_current_temperature(n):
    """_summary_

    Args:
        n (_type_): _description_

    Returns:
        _type_: _description_
    """
    df_current = pd.DataFrame(columns=["city", "temperature", "timestamp"])
    for city in get_all_city_names():
        # should have something liek get_all_current_temperatures
        out = dblAPI.get_current_temperature(city)
        if out is not None:
            temperature = out[0]
            timestamp = out[1]
            if len(df_current) > 0:
                df_current = pd.concat(
                    [
                        df_current,
                        pd.DataFrame(
                            [[city, temperature, timestamp]],
                            columns=["city", "temperature", "timestamp"],
                        ),
                    ],
                    ignore_index=True,
                )
            else:
                df_current = pd.DataFrame(
                    [[city, temperature, timestamp]],
                    columns=["city", "temperature", "timestamp"],
                )
    df_max = pd.DataFrame(columns=["city", "temperature", "timestamp"])
    results = dblAPI.get_all_max_temperatures()
    for result in results:
        temperature, city = result
        timestamps = dblAPI.get_max_temperature_timestamps(city, temperature)
        df_max = pd.concat(
            [
                df_max,
                pd.DataFrame(
                    [[city, temperature, timestamps]],
                    columns=["city", "temperature", "timestamp"],
                ),
            ],
            ignore_index=True,
        )
    fig = px.bar(df_max, x="city", y="temperature")
    return [df_current.to_dict("records"), fig]


print(dt.datetime.now())
if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port="8050")
