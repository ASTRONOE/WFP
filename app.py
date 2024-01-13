import dash
from dash import dcc, html
import plotly.express as px
from countryapi import worldmap, CountryData

# Foremost requirement: Set page config
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# All countries ISO in the WFP dataset
all_countriesIso = list(countryapi.df['countryiso3'])
# All country objects with their ISO
all_countriesObj = CountryData.get_some_countries(all_countriesIso)

# Load world map
world = worldmap()
# Get ISO codes of countries in allcountriesObj
iso_codes = [country_iso for country_iso in all_countriesObj.keys()]
# Filter world map based on ISO codes
filtered_world = world[world["adm0_iso"].isin(iso_codes)]

# Create choropleth map using Plotly Express
fig = px.choropleth(
    filtered_world, geojson=filtered_world.geometry,
    locations=filtered_world.index, color="adm0_iso"
)

# App layout
app.layout = html.Div(children=[
    html.H1("Global Food Price Tracker and Explorer"),
    html.Hr(),
    html.H2("Exploring Worldwide Crop Prices: Tracking Trends And Variations Across Different Regions"),
    html.Hr(),
    dcc.Graph(figure=fig),
])

if __name__ == '__main__':
    app.run(debug=True)
