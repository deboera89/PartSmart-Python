from flask import Flask, render_template, request, redirect, url_for, flash
from dash import Dash, dcc, html, Input, Output, State, callback_context, no_update
import pandas as pd
from sqlalchemy import create_engine, text
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import database_utils  # Import the utility module
# Initialize the Flask app
flask_app = Flask(__name__)
flask_app.secret_key = "supersecretkey"

# Get the Heroku database URL
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/downtime_data")
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
            display_format='YYYY-MM-DD'
        ), style={'textAlign': 'center', 'marginBottom': '10px'}),

        # Buttons for Date Selection
        html.Div([
            html.Button("All Data", id="earliest-date-button", n_clicks=0),
            html.Button("Monthly", id="start-of-month-button", n_clicks=0),
            html.Button("Weekly", id="today-button", n_clicks=0)
        ], style={'textAlign': 'center', 'marginBottom': '20px'}),

        # Checklist Toggle Button
        html.Div([
            html.Button("Select Machine Code(s)", id="toggle-button", n_clicks=0)
        ], style={'textAlign': 'center', 'marginBottom': '20px'}),

        # Checklist container with Select All/None buttons
        html.Div([
            # Buttons container
            html.Div([
                html.Button("Select All", id="select-all-button", n_clicks=0,
                            style={'marginRight': '10px'}),
                html.Button("Deselect All", id="deselect-all-button", n_clicks=0)
            ], style={'marginBottom': '10px', 'textAlign': 'center'}),

            # Checklist
            dcc.Checklist(id='mc-checklist', options=[], value=[])
        ], id='checklist-container', style={'display': 'none'}),

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

    @app.callback(
        [Output('mc-checklist', 'options'),
         Output('mc-checklist', 'value'),
         Output('date-picker-range', 'start_date')],
        [Input('date-picker-range', 'end_date'),
         Input('select-all-button', 'n_clicks'),
         Input('deselect-all-button', 'n_clicks'),
         Input('start-of-month-button', 'n_clicks'),
         Input('today-button', 'n_clicks'),
         Input('earliest-date-button', 'n_clicks')],  # "All Data" button as an input
        [State('mc-checklist', 'options'),
         State('mc-checklist', 'value')]
    )
    def handle_checklist_updates(end_date, select_clicks, deselect_clicks, start_of_month_clicks, today_clicks, earliest_date_click,
                                 current_options, current_value):
        ctx = callback_context
        triggered_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None

        # Load options and start date dynamically from the database
        query = """
            SELECT date, downtime_reason, day, shift_code, mc
            FROM downtime
            WHERE day <> 'Saturday' AND day <> 'Sunday'
            ORDER BY mc;
        """

        df = pd.read_sql(query, con=engine)
        df['date'] = pd.to_datetime(df['date'], dayfirst=True)

        mc_options = [{'label': mc, 'value': mc} for mc in sorted(df['mc'].unique())]
        earliest_date = df['date'].min().date()

        # Set start date based on button clicks
        if triggered_id == 'start-of-month-button':
            start_date = datetime.today().date() - timedelta(days=30)  # One month ago
        elif triggered_id == 'today-button':
            start_date = datetime.today().date() - timedelta(days=7)  # 7 days ago
        elif triggered_id == 'earliest-date-button':
            start_date = earliest_date  # Set to earliest date in the database
        else:
            start_date = earliest_date  # Default to earliest date

        # Handle select/deselect button clicks and initial load
        if triggered_id == 'select-all-button':
            selected_values = [option['value'] for option in mc_options]
        elif triggered_id == 'deselect-all-button':
            selected_values = []
        else:
            # If current_value is None (initial load) or empty, select all options
            if current_value is None or len(current_value) == 0:
                selected_values = [option['value'] for option in mc_options]
            else:
                # Otherwise preserve current selection state
                selected_values = current_value

        return mc_options, selected_values, start_date

    @app.callback(
        Output('downtime-graph', 'figure'),
        [Input('date-picker-range', 'start_date'),
         Input('date-picker-range', 'end_date'),
         Input('mc-checklist', 'value'),
         Input('select-all-button', 'n_clicks'),
         Input('deselect-all-button', 'n_clicks')]
    )
    def update_graph(start_date, end_date, selected_mcs, select_clicks, deselect_clicks):
        if not start_date or not end_date:
            return go.Figure()

        # Check if any machine codes are selected
        if not selected_mcs:
            # Return an empty figure if no checkboxes are selected
            return go.Figure()

        query = f"""
            SELECT date, downtime_reason, day, shift_code, mc
            FROM downtime
            WHERE day <> 'Saturday' AND day <> 'Sunday'
            AND date >= '{start_date}' AND date <= '{end_date}';
        """
        df = pd.read_sql(query, con=engine)

        # Filter data based on selected machine codes
        df = df[df['mc'].isin(selected_mcs)]

        if df.empty:
            return go.Figure()

        downtime_counts = df['downtime_reason'].value_counts().reset_index()
        downtime_counts.columns = ['downtime_reason', 'count']
        day_counts = df['day'].value_counts().reset_index()
        day_counts.columns = ['day', 'count']
        shift_counts = df['shift_code'].value_counts().reset_index()
        shift_counts.columns = ['shift_code', 'count']

        fig = make_subplots(
            rows=2, cols=2,
            row_heights=[0.6, 0.4],
            subplot_titles=("Count of Each Downtime Reason", "Downtime by Day", "Downtime by Shift"),
            specs=[[{"type": "treemap", "colspan": 1}, None], [{"type": "pie"}, {"type": "pie"}]]
        )

        fig.add_trace(go.Treemap(labels=downtime_counts['downtime_reason'],
                                 parents=[""] * len(downtime_counts),
                                 values=downtime_counts['count']), row=1, col=1)
        fig.add_trace(go.Pie(labels=day_counts['day'], values=day_counts['count'], hole=0.3), row=2, col=1)
        fig.add_trace(go.Pie(labels=shift_counts['shift_code'], values=shift_counts['count'], hole=0.3), row=2, col=2)
        fig.update_layout(title_text="Downtime Analysis", showlegend=False, height=800)

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
    flask_app.run(debug=True)
