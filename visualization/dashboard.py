import pandas as pd
import datetime as dt
import plotly.express as px
from dash import Dash
from dash import html
from dash import dash_table
from dash import callback
from dash import Output
from dash import Input
from dash import dcc

from southbayweather.database.queries import get_all_city_names
from southbayweather.database.dbAPI import MySQLAPIFactory

mysqlAPI = MySQLAPIFactory()

app = Dash(__name__)

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
    print("update")
    df_current = pd.DataFrame(columns=["city", "temperature", "timestamp"])
    for city in get_all_city_names():
        temperature, timestamp = mysqlAPI.get_current_temperature(city)
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

    df_max = pd.DataFrame(columns=["city", "temperature", "timestamp"])
    results = mysqlAPI.get_all_max_temperatures()
    for result in results:
        temperature, city = result
        timestamps = mysqlAPI.get_max_temperature_timestamps(city, temperature)
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
    app.run(debug=True, host="localhost", port="8081")
