# import libraries
import dash
import plotly.express as px
import json
import plotly.graph_objects as go
import plotly.io as pio
import countryapi
import pandas as pd
import geopandas as gpd
import dash_daq as daq
from flask_caching import Cache
from countryapi import CountryData
from dash import Dash, dcc, html, Input, Output, Patch, callback

# set up app
external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]
pio.renderers.default = "browser+iframe+iframe_connected"
app = Dash(
    __name__,
    title="Global Food Price Tracker and Explorer",
    external_stylesheets=external_stylesheets,
)

# flask cache_config
cache = Cache(
    config={
        "CACHE_TYPE": "FileSystemCache",
        "CACHE_DIR": "cache-directory",
        "CACHE_THRESHOLD": 200,
    },
)
cache.init_app(app.server)

# load data
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

# filter world map based on ISO codes
iso_codes = list(all_countriesObj.keys())
filtered_world = world[world["adm0_iso"].isin(iso_codes)]
remaining_world = world[~world["adm0_iso"].isin(iso_codes)]

# prepare figure
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
        html.Main(
            children=[
                dcc.Graph(id="world-map", figure=fig_globe),
                daq.BooleanSwitch(id="projection-switch", on=True),
            ]
        ),
    ]
)


@cache.memoize(100)
@callback(
    Output("world-map", "figure"), Input("projection-switch", "on"),
)
def load_world_map(on: bool):
    projection = "mercator" if on else "orthographic"
    fig_globe.update_geos(projection_type=projection, fitbounds="geojson")
    return fig_globe


app.run(debug=True)
