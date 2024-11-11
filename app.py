from flask import Flask, render_template, request, redirect, url_for, flash
from dash import Dash, dcc, html, Input, Output, State, callback_context, no_update
import pandas as pd
from setuptools.command.build_ext import if_dl
from sqlalchemy import create_engine, text
from config import DATABASE_URL
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import database_utils  # Import the utility module
# Initialize the Flask app
flask_app = Flask(__name__)
flask_app.secret_key = "supersecretkey"




engine = create_engine(DATABASE_URL)

def create_dash_app(flask_app):
    app = Dash(__name__, server=flask_app, url_base_pathname='/dashboard/')

    # Dash Layout
    app.layout = html.Div([
        dcc.Store(id='clicked-downtime-reason', storage_type='session'),
        html.H1("Interactive Downtime Analysis"),

        # Date Picker
        html.Div(dcc.DatePickerRange(
            id='date-picker-range',
            start_date=datetime.today().replace(day=1),
            end_date=datetime.today().date(),
            display_format='DD-MM-YYYY'
        ), style={'textAlign': 'center', 'marginBottom': '10px'}),

        # Buttons for Date Selection
        html.Div([
            html.Button("All Data", id="earliest-date-button", n_clicks=0, style={'marginRight': '10px'} ),
            html.Button("Monthly", id="start-of-month-button", n_clicks=0, style={'marginRight': '10px'}),
            html.Button("Weekly", id="today-button", n_clicks=0)
        ], style={'textAlign': 'center', 'marginBottom': '20px'}),

        # Container for both toggle buttons side by side
        html.Div([
            # Machine Code Toggle Button
            html.Div([
                html.Button("Select Machine Code(s)", id="toggle-button", n_clicks=0)
            ], style={'display': 'inline-block', 'marginRight': '20px'}),

            # Day Toggle Button
            html.Div([
                html.Button("Select Day(s)", id="toggle-day-button", n_clicks=0)
            ], style={'display': 'inline-block', 'marginRight': '20px'}),

            # Downtime Reason Toggle Button
            html.Div([
                html.Button("Select Downtime Reason(s)", id="toggle-reason-button", n_clicks=0)
            ], style={'display': 'inline-block'})
        ], style={'textAlign': 'center', 'marginBottom': '20px'}),

        # Rest of your layout remains the same...
        html.Div([
            html.Div([
                html.Button("Select All", id="select-all-button", n_clicks=0,
                            style={'marginRight': '10px'}),
                html.Button("Deselect All", id="deselect-all-button", n_clicks=0)
            ], style={'marginBottom': '10px', 'textAlign': 'center'}),
            dcc.Checklist(id='mc-checklist', options=[], value=[])
        ], id='checklist-container', style={'display': 'none'}),

        html.Div([
            html.Div([
                html.Button("Select All", id="select-all-reason-button", n_clicks=0, style={'marginRight': '10px'}),
                html.Button("Deselect All", id="deselect-all-reason-button", n_clicks=0)
            ], style={'marginBottom': '10px', 'textAlign': 'center'}),
            dcc.Checklist(id='reason-checklist', options=[], value=[])
        ], id='reason-checklist-container', style={'display': 'none'}),

        html.Div([
            html.Div([
                html.Button("Select All", id="select-all-day-button", n_clicks=0, style={'marginRight': '10px'}),
                html.Button("Deselect All", id="deselect-all-day-button", n_clicks=0)
            ], style={'marginBottom': '10px', 'textAlign': 'center'}),
            dcc.Checklist(id='day-checklist', options=[], value=[])
        ], id='day-checklist-container', style={'display': 'none'}),


        dcc.Graph(id='downtime-graph')
    ])


    # Callback to toggle visibility of the checklist container
    @app.callback(
        Output('checklist-container', 'style'),
        Input('toggle-button', 'n_clicks')
    )
    def toggle_visibility(n_clicks):
        if n_clicks and n_clicks % 2 == 1:
            return {
                'display': 'block',
                'maxHeight': '300px',
                'overflowY': 'scroll',
                'columnCount': 6,
                'marginBottom': '20px',
                'padding': '10px',
                'border': '1px solid #ddd',
                'borderRadius': '5px'
            }
        return {'display': 'none'}

    # Callback to toggle visibility of the checklist container
    @app.callback(
        Output('day-checklist-container', 'style'),
        Input('toggle-day-button', 'n_clicks')
    )
    def toggle_visibility(n_clicks):
        if n_clicks and n_clicks % 2 == 1:
            return {
                'display': 'block',
                'maxHeight': '300px',
                'overflowY': 'scroll',
                'columnCount': 8,
                'marginBottom': '20px',
                'padding': '10px',
                'border': '1px solid #ddd',
                'borderRadius': '5px'
            }
        return {'display': 'none'}

    # Callback to toggle visibility of the downtime reason checklist
    @app.callback(
        Output('reason-checklist-container', 'style'),
        Input('toggle-reason-button', 'n_clicks')
    )
    def toggle_reason_visibility(n_clicks):
        if n_clicks and n_clicks % 2 == 1:
            return {
                'display': 'block',
                'maxHeight': '300px',
                'overflowY': 'scroll',
                'columnCount': 6,
                'marginBottom': '20px',
                'padding': '10px',
                'border': '1px solid #ddd',
                'borderRadius': '5px'
            }
        return {'display': 'none'}

    # Updated callback to ensure "Select All" includes all days, including weekends
    @app.callback(
        [Output('mc-checklist', 'options'),
         Output('mc-checklist', 'value'),
         Output('reason-checklist', 'options'),
         Output('reason-checklist', 'value'),
         Output('day-checklist', 'options'),
         Output('day-checklist', 'value'),
         Output('date-picker-range', 'start_date')],
        [Input('date-picker-range', 'end_date'),
         Input('select-all-button', 'n_clicks'),
         Input('deselect-all-button', 'n_clicks'),
         Input('select-all-reason-button', 'n_clicks'),
         Input('deselect-all-reason-button', 'n_clicks'),
         Input('select-all-day-button', 'n_clicks'),
         Input('deselect-all-day-button', 'n_clicks'),
         Input('start-of-month-button', 'n_clicks'),
         Input('today-button', 'n_clicks'),
         Input('earliest-date-button', 'n_clicks')],
        [State('mc-checklist', 'options'),
         State('mc-checklist', 'value'),
         State('day-checklist', 'options'),
         State('day-checklist', 'value'),
         State('reason-checklist', 'options'),
         State('reason-checklist', 'value')]
    )
    def handle_checklist_updates(end_date, select_mc_clicks, deselect_mc_clicks,
                                 select_reason_clicks, deselect_reason_clicks,
                                 select_day_clicks, deselect_day_clicks,
                                 start_of_month_clicks, today_clicks, earliest_date_click,
                                 current_mc_options, current_mc_value,
                                 current_day_options, current_day_value,
                                 current_reason_options, current_reason_value):

        ctx = callback_context
        triggered_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None

        # Query database for options
        query = """
            SELECT date, downtime_reason, mc, day
            FROM downtime
            ORDER BY mc;
        """
        df = pd.read_sql(query, con=engine)

        # Unique options
        mc_options = [{'label': mc, 'value': mc} for mc in sorted(df['mc'].unique())]
        reason_options = [{'label': reason, 'value': reason} for reason in sorted(df['downtime_reason'].unique())]

        # Define chronological order for days
        ordered_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        day_options = [{'label': day, 'value': day} for day in ordered_days if day in df['day'].unique()]

        earliest_date = df['date'].min().date()

        # Handle machine code selection based on button clicks
        if triggered_id == 'select-all-button' or triggered_id is None:
            selected_mcs = [option['value'] for option in mc_options]
        elif triggered_id == 'deselect-all-button':
            selected_mcs = []
        else:
            selected_mcs = current_mc_value or [option['value'] for option in mc_options]

        # Handle reason selection based on button clicks
        if triggered_id == 'select-all-reason-button':
            selected_reasons = [option['value'] for option in reason_options]
        elif triggered_id == 'deselect-all-reason-button':
            selected_reasons = []
        elif triggered_id is None:
            # On page load, select only weekdays
            selected_reasons = [option['value'] for option in reason_options if option['value'] not in ['No Run Scheduled', ]]
        else:
            selected_reasons = current_reason_value or [option['value'] for option in reason_options if
                                                        option['value'] not in ['No Run Scheduled', ]]

        # Handle day selection based on button clicks
        if triggered_id == 'select-all-day-button':
            # Select all days, including Saturday and Sunday
            selected_day = [option['value'] for option in day_options]
        elif triggered_id == 'deselect-all-day-button':
            selected_day = []
        elif triggered_id is None:
            # On page load, select only weekdays
            selected_day = [option['value'] for option in day_options if option['value'] not in ['Saturday', 'Sunday']]
        else:
            # Preserve current selection or default to weekdays if no selection
            selected_day = current_day_value or [option['value'] for option in day_options if
                                                 option['value'] not in ['Saturday', 'Sunday']]

        # Set start date based on button clicks
        start_date = earliest_date
        if triggered_id == 'start-of-month-button':
            start_date = datetime.today().date() - relativedelta(months=1)
        elif triggered_id == 'today-button':
            start_date = datetime.today().date() - timedelta(days=7)
        elif triggered_id == 'earliest-date-button':
            start_date = earliest_date

        return mc_options, selected_mcs, reason_options, selected_reasons, day_options, selected_day, start_date

    @app.callback(
        Output('downtime-graph', 'figure'),
        [Input('date-picker-range', 'start_date'),
         Input('date-picker-range', 'end_date'),
         Input('mc-checklist', 'value'),
         Input('day-checklist', 'value'),
         Input('reason-checklist', 'value')]
    )
    def update_graph(start_date, end_date, selected_mcs, selected_day, selected_reasons):
        # Return an empty figure if no options are selected
        if not start_date or not end_date or not selected_mcs or not selected_reasons or not selected_day:
            return go.Figure()

        # Create the parameter dictionary
        params = {
            'start_date': start_date,
            'end_date': end_date,
            'selected_mcs': tuple(selected_mcs),
            'selected_day': tuple(selected_day),
            'selected_reasons': tuple(selected_reasons)
        }

        # Rest of your code remains the same
        query = text("""
            SELECT date, downtime_reason, mc
            FROM downtime
            WHERE day IN :selected_day
            AND date >= :start_date 
            AND date <= :end_date
            AND mc IN :selected_mcs
            AND downtime_reason IN :selected_reasons
        """)

        df = pd.read_sql(query, con=engine, params=params)

        if df.empty:
            return go.Figure()

        grouped_data = df.groupby('downtime_reason').size().reset_index(name='count')

        fig = go.Figure(go.Treemap(
            labels=grouped_data['downtime_reason'],
            parents=['Total Downtime' for _ in range(len(grouped_data))],
            values=grouped_data['count'],
            textinfo='label+value',
            hovertemplate="""
                Downtime Reason: %{label}<br>
                Count: %{value}<br>
                <extra></extra>
            """
        ))

        return fig

    return app


# Create the Dash app and assign it to a global variable
dash_app = create_dash_app(flask_app)

# Flask routes
@flask_app.route('/')
def index():
    return render_template('dashboard.html')

@flask_app.route('/upload')
def upload():
    return render_template('upload.html')

@flask_app.route('/upload_file', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file part')
        return redirect(url_for('upload'))

    file = request.files['file']
    if file.filename == '':
        flash('No selected file')
        return redirect(url_for('upload'))

    if not file.filename.endswith('.csv'):
        flash('Invalid file format. Please upload a CSV file.')
        return redirect(url_for('upload'))

    try:
        database_utils.setup_and_insert_data(file)
        flash("File uploaded and data successfully stored in the database.")
    except Exception as e:
        flash(f"An error occurred: {e}")
        return redirect(url_for('upload'))

    return redirect(url_for('index'))


if __name__ == "__main__":
    # Get the port from the environment variable or default to 5000 for local development
    port = int(os.getenv("PORT", 5000))
    flask_app.run(host="0.0.0.0", port=port, debug=True)
