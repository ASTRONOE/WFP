import dash
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import countryapi
import pandas as pd
import geopandas as gpd
from countryapi import CountryData
from dash import Dash, dcc, html, Input, Output, callback
from flask_caching import Cache


external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]
app = Dash(__name__, external_stylesheets=external_stylesheets)
pio.renderers.default = "browser+iframe+iframe_connected"


cache_config = Cache(
    config={"DEBUG": True, "CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
)
cache_config.init_app(app.server)

wfp = pd.read_csv("assets/wfp_countries_global_2.csv", usecols=["countryiso3"])
world = gpd.read_feather(
    "assets/worldmap.feather",
    columns=[
        "sovereignt",
        "sov_a3",
        "level",
        "adm0_iso",
        "admin",
        "name",
        "name_long",
        "brk_a3",
        "brk_name",
        "abbrev",
        "geometry",
    ],
)

all_countriesIso = list(wfp["countryiso3"])
all_countriesObj = CountryData.get_some_countries(all_countriesIso)
iso_codes = list(all_countriesObj.keys())


# Filter world map based on ISO codes
filtered_world = world[world["adm0_iso"].isin(iso_codes)]
remaining_world = world[~world["adm0_iso"].isin(iso_codes)]

fig_globe = go.Figure()
fig_globe.add_traces(
    [
        go.Choropleth(
            geojson=filtered_world.geometry.__geo_interface__,
            locations=filtered_world.index,
            colorscale=[[0, "rgb(245, 10, 10)"], [1, "rgb(245, 10, 10)"]],
            z=[1] * len(filtered_world),
            showscale=False,
        ),
        go.Choropleth(
            geojson=remaining_world.geometry.__geo_interface__,
            locations=remaining_world.index,
            colorscale=[[0, "rgb(211, 211, 211)"], [1, "rgb(211, 211, 211)"]],
            z=[2] * len(remaining_world),
            showscale=False,
        ),
    ]
)

fig_globe.update_geos(projection_type="mercator", fitbounds="geojson")
fig_globe.update_layout(autosize=False, height=1800, width=1400)


app.layout = html.Div(
    children=[
        html.Header(
            [
                html.H1("Global Food Price Tracker and Explorer"),
                html.Hr(),
                html.H2(
                    "Exploring Worldwide Crop Prices: Tracking Trends And Variations Across Different Regions"
                ),
                html.Hr(),
            ]
        ),
        html.Div(children=[dcc.Graph(figure=fig_globe)]),
    ]
)

app.run(debug=True)
