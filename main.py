from flask import Flask, render_template
from flask_socketio import SocketIO, emit
import pandas as pd
import time
import threading
import os
from datetime import datetime
from urllib.parse import quote

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY',
                                     'fallback-secret-key-for-development')
socketio = SocketIO(app, cors_allowed_origins="*")

# Global variables to store count data
count_data = {
    'departments': [],
    'total_alumni': 0,
    'male_count': 0,
    'female_count': 0,
    'last_updated': None
}


def get_worksheet_as_dataframe(sheet_url, sheet_name=None):
    """
    Fetch data from specific Google Sheets tab and convert to pandas DataFrame
    """
    try:
        # Extract sheet ID from URL
        if '/edit' in sheet_url:
            sheet_id = sheet_url.split('/d/')[1].split('/')[0]
        else:
            sheet_id = sheet_url.split('/')[-2] if '/' in sheet_url else sheet_url

        # If specific sheet name is provided, URL-encode the name
        if sheet_name:
            encoded_sheet_name = quote(sheet_name)  # Encode special characters
            csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={encoded_sheet_name}"
        else:
            csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"

        # Read the CSV data
        df = pd.read_csv(csv_url)

        # Clean the data
        df = df.dropna(how='all')  # Remove completely empty rows
        return df

    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()


def check_for_updates():
    """
    Check for updates in the Count sheet
    """
    global count_data

    # Your Google Sheets URL
    WORKSHEET_URL = "https://docs.google.com/spreadsheets/d/1DNcOHB334c9H2QZ24CsGasgam4sj-WkpPOgvyn4yPzg/edit?usp=sharing"

    while True:
        try:
            # Fetch data from "Count" sheet
            df = get_worksheet_as_dataframe(WORKSHEET_URL, "LIVE COUNT")

            if not df.empty:
                current_time = datetime.now()

                # Process department data (first 19 rows)
                departments = []
                total_male = 0
                total_female = 0
                total_overall = 0

                # Assuming columns are: Department, Male Count, Female Count, Total Count
                for index, row in df.head(
                        21).iterrows():  # First 19 rows for departments
                    if pd.notna(
                            row.iloc[0]):  # Check if department name exists
                        dept_data = {
                            'department':
                            str(row.iloc[0])
                            if pd.notna(row.iloc[0]) else 'Unknown',
                            'male_count':
                            int(row.iloc[1]) if pd.notna(row.iloc[1])
                            and str(row.iloc[1]).isdigit() else 0,
                            'female_count':
                            int(row.iloc[2]) if pd.notna(row.iloc[2])
                            and str(row.iloc[2]).isdigit() else 0,
                            'total_count':
                            int(row.iloc[3]) if pd.notna(row.iloc[3])
                            and str(row.iloc[3]).isdigit() else 0
                        }
                        departments.append(dept_data)
                        total_male += dept_data['male_count']
                        total_female += dept_data['female_count']
                        total_overall += dept_data['total_count']

                # Try to get overall totals from the sheet (last row with overall data)
                if len(df) > 19:
                    # Look for the row with overall totals
                    overall_row = None
                    for index, row in df.tail(
                            5).iterrows():  # Check last 5 rows
                        if 'total' in str(
                                row.iloc[0]).lower() or 'overall' in str(
                                    row.iloc[0]).lower():
                            overall_row = row
                            break

                    if overall_row is not None:
                        # Extract overall totals from the sheet
                        sheet_total = int(
                            overall_row.iloc[3]
                        ) if pd.notna(overall_row.iloc[3]) and str(
                            overall_row.iloc[3]).isdigit() else total_overall
                        sheet_male = int(overall_row.iloc[1]) if pd.notna(
                            overall_row.iloc[1]) and str(
                                overall_row.iloc[1]).isdigit() else total_male
                        sheet_female = int(
                            overall_row.iloc[2]
                        ) if pd.notna(overall_row.iloc[2]) and str(
                            overall_row.iloc[2]).isdigit() else total_female

                        total_overall = sheet_total
                        total_male = sheet_male
                        total_female = sheet_female

                # Update global count data
                new_count_data = {
                    'departments': departments,
                    'total_alumni': total_overall,
                    'male_count': total_male,
                    'female_count': total_female,
                    'last_updated': current_time.strftime('%Y-%m-%d %H:%M:%S')
                }

                # Check if data has changed
                if new_count_data != count_data:
                    count_data = new_count_data

                    # Emit updated data to all connected clients
                    socketio.emit(
                        'count_update', {
                            'departments': count_data['departments'],
                            'total_alumni': count_data['total_alumni'],
                            'male_count': count_data['male_count'],
                            'female_count': count_data['female_count'],
                            'last_updated': count_data['last_updated']
                        })

                    print(
                        f"Count data updated at {count_data['last_updated']} - Total: {total_overall}, Male: {total_male}, Female: {total_female}"
                    )

        except Exception as e:
            print(f"Error in check_for_updates: {e}")

        time.sleep(5)  # Check every 10 seconds


@app.route('/')
def dashboard():
    return render_template('dashboard.html')


@app.route('/api/count')
def get_count_data():
    return count_data


@socketio.on('connect')
def handle_connect():
    print('Client connected')
    emit(
        'count_update', {
            'departments': count_data['departments'],
            'total_alumni': count_data['total_alumni'],
            'male_count': count_data['male_count'],
            'female_count': count_data['female_count'],
            'last_updated': count_data['last_updated']
        })


@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')


if __name__ == '__main__':
    # Start the background thread for checking updates
    update_thread = threading.Thread(target=check_for_updates, daemon=True)
    update_thread.start()

    # Run the Flask-SocketIO app
    socketio.run(app,
                 host='0.0.0.0',
                 port=5000,
                 debug=True,
                 allow_unsafe_werkzeug=True)
