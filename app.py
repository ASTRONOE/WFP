import dash
from dash import Dash, dcc, html
import plotly.express as px
import countryapi
from countryapi import CountryData

#Foremost requirement: Set page config
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = Dash(__name__, external_stylesheets=external_stylesheets)

wfp = countryapi.wfp
world = countryapi.world

#All countries ISO in the WFP dataset
all_countriesIso = list(wfp['countryiso3'])
#All country objects with their ISO
all_countriesObj = CountryData.get_some_countries(all_countriesIso)

# # Get ISO codes of countries in allcountriesObj
iso_codes = [country_iso for country_iso in all_countriesObj.keys()]
# # Filter world map based on ISO codes
filtered_world = world[world["adm0_iso"].isin(iso_codes)]

#Create choropleth map using Plotly Express
fig = px.choropleth(
  filtered_world, geojson=filtered_world.geometry,
  locations=filtered_world.index, color="adm0_iso"
)

fig.show()
# App layout
app.layout = html.Div(children=[
  html.Header([
    html.H1("Global Food Price Tracker and Explorer"),
    html.Hr(),
    html.H2("Exploring Worldwide Crop Prices: Tracking Trends And Variations Across Different Regions"),
    html.Hr()
  ]),

  html.Div([dcc.Graph(figure=fig)], style={'width': '70%', 'margin': 'auto'}),
])

if __name__ == '__main__':
  app.run(host='0.0.0.0', port='8080', debug=True)