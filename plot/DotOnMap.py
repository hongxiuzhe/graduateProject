import plotly

plotly.tools.set_credentials_file(username='hongxiuzhe', api_key='eM22bpQX6mB7PtKvlrxY')

# plotly.tools.set_config_file(world_readable=False,
#                              sharing='private')
# plotly.tools.set_config_file(plotly_domain='https://plotly.your-company.com',
#                              plotly_api_domain='https://plotly.your-company.com',
#                              plotly_streaming_domain='stream-plotly.your-company.com')

import plotly.plotly as py
from plotly.graph_objs import *

# mapbox_access_token = 'sk.eyJ1IjoiaG9uZ3hpdXpoZSIsImEiOiJjajc2YnV0ZzUxMXJqMzFqd3Z6dXpyYmdtIn0.xz0Y5UmUYUc08sZjVQ52PA'
mapbox_access_token = 'pk.eyJ1IjoiaGVsbG9iYW5nIiwiYSI6ImNqN2FkdXlnYzBlaDEzMXFwd2QzN3pjNXUifQ.LS_wh7iYh5QrfSjupmVdFg'

data = Data([
    Scattermapbox(
        lat=['38.91427','38.91538','38.91458',
             '38.92239','38.93222','38.90842',
             '38.91931','38.93260','38.91368',
             '38.88516','38.921894','38.93206',
             '38.91275'],
        lon=['-77.02827','-77.02013','-77.03155',
             '-77.04227','-77.02854','-77.02419',
             '-77.02518','-77.03304','-77.04509',
             '-76.99656','-77.042438','-77.02821',
             '-77.01239'],
        mode='markers',
        marker=Marker(
            size=9
        ),
        text=["The coffee bar","Bistro Bohem","Black Cat",
             "Snap","Columbia Heights Coffee","Azi's Cafe",
             "Blind Dog Cafe","Le Caprice","Filter",
             "Peregrine","Tryst","The Coupe",
             "Big Bear Cafe"],
    )
])
layout = Layout(
    autosize=True,
    hovermode='closest',
    mapbox=dict(
        accesstoken=mapbox_access_token,
        bearing=0,
        center=dict(
            lat=38.92,
            lon=-77.07
        ),
        pitch=0,
        zoom=10
    ),
)

fig = dict(data=data, layout=layout)
py.plot(fig, filename='Multiple Mapbox')