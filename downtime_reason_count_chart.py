from dash import Dash, dcc, html, Input, Output, State
import dash
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

# Database connection details
DATABASE_URL = 'postgresql://postgres:1T1I1m1e@localhost/downtime_data'
engine = create_engine(DATABASE_URL)

# Query the data
query = """
    SELECT date, downtime_reason, day, shift_code, mc
    FROM downtime
    WHERE day <> 'Saturday' AND day <> 'Sunday';
"""
df = pd.read_sql(query, con=engine)
df['date'] = pd.to_datetime(df['date'], dayfirst=True)

# Get unique machine codes for the checklist options
mc_options = [{'label': mc, 'value': mc} for mc in df['mc'].unique()]
all_mc_values = [mc['value'] for mc in mc_options]  # Get all machine code values
earliest_date = df['date'].min().date() # get the earliest date in the data set




# Initialize the Dash app
app = Dash(__name__)

# Get the current date and the first day of the current month
today = datetime.today().date()
first_day_of_month = today.replace(day=1)

# Layout of the Dash app with date picker on one line and buttons below it
app.layout = html.Div([
    # Hidden store to hold clicked downtime reason
    dcc.Store(id='clicked-downtime-reason', storage_type='session'),


    html.H1("Interactive Downtime Analysis"),

    dcc.ConfirmDialog(
        id='alert-dialog',
        message=""
    ),

    # Date Picker on its own line
    html.Div(
        dcc.DatePickerRange(
            id='date-picker-range',
            start_date=first_day_of_month,
            end_date=today,
            display_format='YYYY-MM-DD'
        ),
        style={'textAlign': 'center', 'marginBottom': '10px'}  # Center align and add spacing below
    ),

    # Buttons below the Date Picker
    html.Div([
        html.Button("All Data", id="earliest-date-button", n_clicks=0, style={'marginRight': '10px'}),
        html.Button("Set to Start of Month", id="start-of-month-button", n_clicks=0, style={'marginRight': '10px'}),
        html.Button("Set to Today", id="today-button", n_clicks=0),

    ], style={'textAlign': 'center', 'marginBottom': '20px'}),  # Center align and add spacing below buttons

    # Button to toggle checklist visibility
    html.Div([
        html.Button("Select Machine Code(s)", id="toggle-button", n_clicks=0),
    ], style={'textAlign': 'center', 'marginBottom': '20px'}),  # Center align and add spacing below buttons

    # Checklist container with checkbox items
    html.Div([
        html.Div(
            dcc.Checklist(
                id='mc-checklist',
                options=mc_options,
                value=all_mc_values  # Set all options as selected by default
            ),
            style={'columnCount': 6, 'paddingBottom': '10px'}
        )
    ], id='checklist-container', style={'display': 'none', 'maxHeight': '300px', 'overflowY': 'scroll'}),

    # Div for Buttons to appear below the checklist items
    html.Div([
        html.Button("Select All", id="select-all", n_clicks=0, style={'marginRight': '10px'}),
        html.Button("Deselect All", id="deselect-all", n_clicks=0)
    ], id='button-container', style={'display': 'none', 'textAlign': 'center'}),  # Initially hidden

    dcc.Graph(id='downtime-graph')
])


# Callback to set the date picker to the start of the month or to today
# Callback to set the date picker to the start of the month, today, or earliest date
@app.callback(
    Output('date-picker-range', 'start_date'),
    Output('date-picker-range', 'end_date'),
    Input('start-of-month-button', 'n_clicks'),
    Input('today-button', 'n_clicks'),
    Input('earliest-date-button', 'n_clicks')
)
def update_dates(start_of_month_clicks, today_clicks, earliest_date_clicks):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update, dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'start-of-month-button':
        return first_day_of_month, today  # Set to start of month and today
    elif button_id == 'today-button':
        return today, today  # Set both start and end to today
    elif button_id == 'earliest-date-button':
        return earliest_date, today  # Set start to earliest date and end to today

    return dash.no_update, dash.no_update  # No update if no button is clicked


# Callback to toggle the visibility of both the checklist container and the button container
@app.callback(
    [Output('checklist-container', 'style'),
     Output('button-container', 'style')],
    Input('toggle-button', 'n_clicks')
)
def toggle_visibility(n_clicks):
    # Toggle visibility based on the number of clicks
    if n_clicks % 2 == 1:
        # Show both checklist and buttons
        return {'display': 'block', 'maxHeight': '300px', 'overflowY': 'scroll'}, \
               {'display': 'block', 'textAlign': 'center'}
    else:
        # Hide both checklist and buttons
        return {'display': 'none'}, {'display': 'none'}


# Callback to handle Select All and Deselect All button functionality
@app.callback(
    Output('mc-checklist', 'value'),
    [Input('select-all', 'n_clicks'),
     Input('deselect-all', 'n_clicks')],
    State('mc-checklist', 'value')
)
def update_checklist(select_all_clicks, deselect_all_clicks, current_values):
    # Check which button was clicked most recently
    ctx = dash.callback_context
    if not ctx.triggered:
        return current_values
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'select-all':
        return all_mc_values  # Select all options
    elif button_id == 'deselect-all':
        return []  # Deselect all options

    return current_values  # Return current selection if no button is clicked

@app.callback(
    [Output('alert-dialog', 'message'),
     Output('alert-dialog', 'displayed')],
    Input('downtime-graph', 'clickData')
)
def show_alert(clickData):
    if clickData:
        # Extract details from the clicked bar, such as downtime reason and count
        downtime_reason = clickData['points'][0]['x']
        downtime_count = clickData['points'][0]['y']
        message = f"Downtime Reason: {downtime_reason}\nCount: {downtime_count}"
        return message, True  # Show alert with message
    return "", False  # Hide alert if no click


# Callback to update the graph based on the selected date range and machine code(s)
@app.callback(
    Output('downtime-graph', 'figure'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date'),
    Input('mc-checklist', 'value')
)
def update_graph(start_date, end_date, selected_mcs):
    # If no machines are selected, return an empty figure
    if not selected_mcs:
        return go.Figure()  # Returns an empty figure when no machines are selected

    # Filter the DataFrame based on the selected date range and machine code(s)
    filtered_df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
    filtered_df = filtered_df[filtered_df['mc'].isin(selected_mcs)]

    # Count occurrences of each downtime_reason
    downtime_counts = filtered_df['downtime_reason'].value_counts().reset_index()
    downtime_counts.columns = ['downtime_reason', 'count']

    # Count occurrences of each day
    day_counts = filtered_df['day'].value_counts().reset_index()
    day_counts.columns = ['day', 'count']

    # Count occurrences of each shift
    shift_counts = filtered_df['shift_code'].value_counts().reset_index()
    shift_counts.columns = ['shift_code', 'count']

    # Create a subplot figure with the new layout and adjusted spacing
    fig = make_subplots(
        rows=2, cols=2,
        row_heights=[0.6, 0.4],  # Adjust row heights
        subplot_titles=("Count of Each Downtime Reason", "Percentage of Downtime Occurrences by Day", "Downtime Count per Shift"),
        specs=[[{"type": "bar", "colspan": 2}, None], [{"type": "pie"}, {"type": "pie"}]],
        vertical_spacing=0.5  # Increase vertical space between rows
    )

    # Add bar chart for downtime reasons (spanning two columns in the first row)
    fig.add_trace(
        go.Bar(x=downtime_counts['downtime_reason'], y=downtime_counts['count'], text=downtime_counts['count'], textposition='outside'),
        row=1, col=1
    )

    # Add pie chart for downtime occurrences by day with domain positioning
    fig.add_trace(
        go.Pie(labels=day_counts['day'], values=day_counts['count'], hole=0.3, domain=dict(x=[0, 0.5], y=[0, 0.5])),
        row=2, col=1
    )

    # Add pie chart for downtime occurrences by shift with domain positioning
    fig.add_trace(
        go.Pie(labels=shift_counts['shift_code'], values=shift_counts['count'], hole=0.3, domain=dict(x=[0.5, 1.0], y=[0, 0.5])),
        row=2, col=2
    )

    # Update layout for the entire figure
    fig.update_layout(
        title_text="Downtime Analysis",
        showlegend=False,
        height=800  # Increase figure height
    )

    return fig


# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
