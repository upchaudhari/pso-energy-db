import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, date
from io import BytesIO
import base64
import streamlit as st
import queue
import os
from datetime import datetime

# Function to connect to the SQLite database
def connect_to_database():
    db_connection_str = 'sqlite:///test.db' # Path to the SQLite database
    db_connection = create_engine(db_connection_str)
    return db_connection

# Function to get a list of tables from the database
def get_tables():
    db_connection = connect_to_database()
    query = "SELECT name FROM sqlite_master WHERE type='table'"
    df = pd.read_sql_query(query, con=db_connection)
    return df['name'].tolist()

# Function to get data based on date range and table name
def get_data(table_name, date_from, date_to, selected_meters=None):
    db_connection = connect_to_database()
    columns_to_select = '[Date], [Time], ' + ', '.join([f'[{col}]' for col in selected_meters]) if selected_meters else '*'
    query = f""" SELECT {columns_to_select} FROM {table_name} WHERE [Date] BETWEEN :date_from AND :date_to """
    df = pd.read_sql_query(query, con=db_connection, params={"date_from": date_from, "date_to": date_to})
    return df

# Function to get the range of dates available in the database for a given table
def get_date_range(table_name):
    db_connection = connect_to_database()
    query = f"SELECT MIN([Date]) AS min_date, MAX([Date]) AS max_date FROM {table_name}"
    df = pd.read_sql_query(query, con=db_connection)

    if not df.empty and df.loc[0, 'min_date'] and df.loc[0, 'max_date']:
        min_date = pd.to_datetime(df.loc[0, 'min_date']).date()
        max_date = pd.to_datetime(df.loc[0, 'max_date']).date()
        return min_date, max_date
    else:
        return date.today(), date.today()

# Function to get available dates and times for a given table
def get_available_dates_times(table_name):
    db_connection = connect_to_database()
    query = f"SELECT DISTINCT [Date], [Time] FROM {table_name} ORDER BY [Date], [Time]"
    df = pd.read_sql_query(query, con=db_connection)

    # Handle the case where Date or Time column is missing
    if 'Date' not in df.columns:
        df['Date'] = None
    if 'Time' not in df.columns:
        df['Time'] = None

    available_dates = sorted(df['Date'].dropna().unique().tolist())
    available_times = sorted(df['Time'].dropna().unique().tolist())

    return available_dates, available_times

def get_existing_value(table_name, date_str, time_str, column_name):
    db_connection = connect_to_database()

    # Print the original date and time strings
    print(f"Original date_str: {date_str}, time_str: {time_str}")

    try:
        # The Date field in the database includes both date and time components
        formatted_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        raise ValueError(f"Date format not recognized: {date_str}") from e

    try:
        # The Time field in the database includes microseconds
        formatted_time = datetime.strptime(time_str, '%H:%M:%S.%f').strftime('%H:%M:%S.%f')
    except ValueError as e:
        raise ValueError(f"Time format not recognized: {time_str}") from e

    # Print the formatted date and time for debugging
    print(f"Fetching existing value with date: {formatted_date} and time: {formatted_time}")

    query = f"SELECT [{column_name}] FROM {table_name} WHERE [Date] = :date AND [Time] = :time"
    df = pd.read_sql_query(query, con=db_connection, params={"date": formatted_date, "time": formatted_time})

    # Print the SQL query and the resulting DataFrame
    print(f"SQL Query: {query}")
    print(f"Query Parameters: date={formatted_date}, time={formatted_time}")
    print(f"Query Result: {df}")

    if not df.empty:
        return df[column_name].tolist()
    return []

def update_entry(table_name, date_str, time_str, column_name, new_value):
    db_connection = connect_to_database()
    try:
        # The Date field in the database includes both date and time components
        formatted_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        raise ValueError(f"Date format not recognized: {date_str}") from e
    try:
        # The Time field in the database includes microseconds
        formatted_time = datetime.strptime(time_str, '%H:%M:%S.%f').strftime('%H:%M:%S.%f')
    except ValueError as e:
        raise ValueError(f"Time format not recognized: {time_str}") from e

    # Print the formatted date and time for debugging
    print(f"Updating entry with date: {formatted_date} and time: {formatted_time}")

    # Get the existing value
    existing_value = get_existing_value(table_name, date_str, time_str, column_name)
    print(f"Current value before update: {existing_value}")

    if existing_value:
        existing_value_float = float(existing_value[0])
        if new_value is not None:
            new_value_float = float(new_value)
            if abs(new_value_float - existing_value_float) < 1e-6:
                print("New value is the same as the existing value. No update needed.")
                return existing_value

        query = text(f"UPDATE {table_name} SET [{column_name}] = :new_value WHERE [Date] = :date AND [Time] = :time")
        try:
            with db_connection.connect() as conn:
                print(f"Executing query: {query}")
                print(f"Parameters: new_value={new_value}, date={formatted_date}, time={formatted_time}")
                conn.execute(query, {"new_value": new_value, "date": formatted_date, "time": formatted_time})
                conn.commit() # Commit the changes to the database

            # Log the update
            log_update(table_name, date_str, time_str, column_name, existing_value[0], new_value)

            # Verify the update
            updated_value = get_existing_value(table_name, date_str, time_str, column_name)
            print(f"Updated value after query: {updated_value}")
            return updated_value
        except Exception as e:
            print(f"Error updating entry: {e}")
            return None
    else:
        print(f"No existing value found for {table_name}, {date_str}, {time_str}, {column_name}")
        return None
    
def log_update(table_name, date_str, time_str, column_name, old_value, new_value):
    log_dir = 'update_logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file = os.path.join(log_dir, f"{table_name}_updates.log")
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    with open(log_file, 'a') as f:
        f.write(f"{timestamp} | {table_name} | {date_str} {time_str} | {column_name} | Old Value: {old_value} | New Value: {new_value}\n")


# Function to download data as an Excel file
def download_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    processed_data = output.getvalue()
    b64 = base64.b64encode(processed_data).decode()
    href = f'Download Excel file'
    return href

# Function to read log file and return its content
def read_log_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def print_sample_row(table_name):
    db_connection = connect_to_database()
    query = f"SELECT * FROM {table_name} LIMIT 1"
    df = pd.read_sql_query(query, con=db_connection)
    print(f"Sample row from {table_name}:")
    print(df)

# Call this function at the beginning of your Streamlit app to print a sample row
print_sample_row('electric')

# Streamlit UI
st.title('Database Data Retrieval and Update')

# Tab to display different views
tab1, tab2, tab3, tab4, tab5 = st.tabs(["Data Retrieval", "Update Entry", "Log File", "Update Log", "Sum of Column"])

with tab1:
    # Dropdown to select table
    table_list = get_tables()
    selected_table = st.selectbox('Select a table', table_list)

    if selected_table:
        # Get the date range for the selected table
        min_date, max_date = get_date_range(selected_table)
        st.write(f"Available date range for {selected_table}: {min_date} to {max_date}")

        # Display date input widgets
        date_from = st.date_input("Date from", min_date, min_value=min_date, max_value=max_date)
        date_to = st.date_input("Date to", max_date, min_value=min_date, max_value=max_date)

        if date_from and date_to:
            # Fetch data based on the selected date range
            df = get_data(selected_table, date_from, date_to)

            if not df.empty:
                st.write(f"Data from {date_from} to {date_to}")
                st.write(df)

                # Show available columns (meter names) for selection
                meter_columns = [col for col in df.columns if 'usage' not in col.lower() and col.lower() not in ['date', 'time', 'modification']]
                selected_meters = st.multiselect('Select meters (optional)', meter_columns)

                # If meters are selected, filter the DataFrame
                if selected_meters:
                    filtered_df = df[['Date', 'Time'] + selected_meters]
                    st.write(f"Data for selected meters:", filtered_df)
                else:
                    filtered_df = df

                st.markdown(download_excel(filtered_df), unsafe_allow_html=True)
            else:
                st.write("No data found for the selected date range.")

with tab2:
    # Dropdown to select table
    table_list = get_tables()
    selected_table = st.selectbox('Select a table', table_list, key='tab2_table_select')

    if selected_table:
        st.write("### Update Specific Entry")
        available_dates, available_times = get_available_dates_times(selected_table)
        selected_date_str = st.selectbox("Select Date for Update", available_dates, key='tab2_date_select')
        selected_time_str = st.selectbox("Select Time for Update", available_times, key='tab2_time_select')

        # Get the column names for the selected table
        db_connection = connect_to_database()
        query = f"PRAGMA table_info({selected_table})"
        column_info = pd.read_sql_query(query, con=db_connection)
        column_names = column_info['name'].tolist()

        column_to_update = st.selectbox("Select column to update", column_names, key='tab2_column_select')

        if selected_date_str and selected_time_str and column_to_update:
            existing_values = get_existing_value(selected_table, selected_date_str, selected_time_str, column_to_update)
            st.write(f"Existing value(s): {', '.join(map(str, existing_values))}")
            new_value = st.text_input("Enter new value", key='tab2_new_value')

            if st.button("Update Entry"):
                try:
                    new_value = float(new_value) if new_value else None
                    updated_value = update_entry(selected_table, selected_date_str, selected_time_str, column_to_update, new_value)
                    if new_value is not None and float(new_value) in updated_value:
                        st.success(f"Updated entry on {selected_date_str} at {selected_time_str} in column {column_to_update} with new value '{new_value}'")
                    else:
                        st.error("Failed to update the entry. Please try again.")
                    df = get_data(selected_table, date_from, date_to)
                    st.write(df)
                except ValueError:
                    st.error("Invalid input. Please enter a numeric value.")

with tab3:
    # Display log file
    st.write("### Log File Content")
    log_file_path = 'high_usage_alerts.log'
    log_content = read_log_file(log_file_path)
    st.text_area('Log File', log_content, height=400)

with tab4:
    st.write("### Update Logs")
    table_list = get_tables()
    selected_table = st.selectbox('Select a table', table_list, key='tab4_table_select')
    if selected_table:
        log_file = os.path.join('update_logs', f"{selected_table}_updates.log")
        if os.path.exists(log_file):
            log_content = read_log_file(log_file)
            st.text_area('Update Log', log_content, height=400)
        else:
            st.write(f"No update logs found for {selected_table}")

with tab5:
    st.write("### Calculate Sum of Usage Columns")
    table_list = get_tables()
    selected_table = st.selectbox('Select a table', table_list, key='tab5_table_select')
    if selected_table:
        min_date, max_date = get_date_range(selected_table)
        st.write(f"Available date range for {selected_table}: {min_date} to {max_date}")
        date_from = st.date_input("Date from", min_date, min_value=min_date, max_value=max_date, key='tab5_date_from')
        date_to = st.date_input("Date to", max_date, min_value=min_date, max_value=max_date, key='tab5_date_to')
        if date_from and date_to:
            df = get_data(selected_table, date_from, date_to)
            usage_columns = [col for col in df.columns if col.lower().endswith('_usage')]
            selected_usage_column = st.selectbox('Select usage column', usage_columns, key='tab5_usage_column')
            if selected_usage_column:
                sum_value = df[selected_usage_column].sum()
                st.write(f"Sum of {selected_usage_column} from {date_from} to {date_to}: {sum_value}")

