import sqlite3
import pandas as pd
from pathlib import Path

# Allow running this module directly (adds project root to sys.path when needed)
try:
    from app.data.db import connect_database
except ModuleNotFoundError:
    import sys
    import pathlib as _pl
    sys.path.append(str(_pl.Path(__file__).resolve().parents[2]))
    from app.data.db import connect_database

def load_csv_to_table(conn, csv_path, table_name):
    """
    Load a CSV file into a database table using pandas.
    
    Args:
        conn: Database connection
        csv_path: Path to CSV file
        table_name: Name of the target table
        
    Returns:
        int: Number of rows loaded
    """
    # Convert path to Path object
    csv_file = Path(csv_path)
    
    # Check if CSV file exists
    if not csv_file.is_file():
        print(f"Error: CSV file '{csv_path}' does not exist.")
        return 0
    
    try:
        # Read CSV into a DataFrame
        df = pd.read_csv(csv_file)
        
        # Load DataFrame into the database
        df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
        
        # Success message
        row_count = len(df)
        print(f"Successfully loaded {row_count} rows into '{table_name}'.")
        return row_count
    except Exception as e:
        print(f"Error loading CSV to table: {e}")
        return 0


def load_all_csv_data(conn):
    """
    Load all CSV files from the `DATA` folder into their target tables.

    Returns:
        int: Total number of rows loaded across all files
    """
    data_dir = Path("DATA")

    # Map CSV filenames (as present in the repository) to the DB table names
    files = [
        ("cyber_incedents.csv", "cyber_incidents"),
        ("datasets_metadata.csv", "datasets_metadata"),
        ("it_tickets.csv", "it_tickets"),
    ]

    total_loaded = 0
    for filename, table in files:
        csv_path = data_dir / filename
        rows = load_csv_to_table(conn, csv_path, table)
        total_loaded += rows

    print(f"Total rows loaded from CSVs: {total_loaded}")
    return total_loaded

def get_incidents_by_type_count(conn):
    """
    Count incidents by type.
    Uses: SELECT, FROM, GROUP BY, ORDER BY
    """
    query = """
    SELECT incident_type, COUNT(*) as count
    FROM cyber_incidents
    GROUP BY incident_type
    ORDER BY count DESC
    """
    df = pd.read_sql_query(query, conn)
    return df

def get_high_severity_by_status(conn):
    """
    Count high severity incidents by status.
    Uses: SELECT, FROM, WHERE, GROUP BY, ORDER BY
    """
    query = """
    SELECT status, COUNT(*) as count
    FROM cyber_incidents
    WHERE severity = 'High'
    GROUP BY status
    ORDER BY count DESC
    """
    df = pd.read_sql_query(query, conn)
    return df

def get_incident_types_with_many_cases(conn, min_count=5):
    """
    Find incident types with more than min_count cases.
    Uses: SELECT, FROM, GROUP BY, HAVING, ORDER BY
    """
    query = """
    SELECT incident_type, COUNT(*) as count
    FROM cyber_incidents
    GROUP BY incident_type
    HAVING COUNT(*) > ?
    ORDER BY count DESC
    """
    df = pd.read_sql_query(query, conn, params=(min_count,))
    return df


def insert_incident(conn: sqlite3.Connection, date: str, incident_type: str, severity: str,
                    status: str, description: str, reported_by: str = None) -> int:
    """
    Insert a new cyber incident into the database.
    """
    cursor = conn.cursor()
    sql = """
    INSERT INTO cyber_incidents 
    (date, incident_type, severity, status, description, reported_by)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    cursor.execute(sql, (date, incident_type, severity, status, description, reported_by))
    conn.commit()
    return cursor.lastrowid


def get_all_incidents(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Retrieve all incidents from the database.
    """
    return pd.read_sql_query("SELECT * FROM cyber_incidents", conn)


def update_incident_status(conn: sqlite3.Connection, incident_id: int, new_status: str) -> int:
    """
    Update the status of an incident.
    """
    cursor = conn.cursor()
    sql = "UPDATE cyber_incidents SET status = ? WHERE id = ?"
    cursor.execute(sql, (new_status, incident_id))
    conn.commit()
    return cursor.rowcount


def delete_incident(conn: sqlite3.Connection, incident_id: int) -> int:
    """
    Delete an incident from the database.
    """
    cursor = conn.cursor()
    sql = "DELETE FROM cyber_incidents WHERE id = ?"
    cursor.execute(sql, (incident_id,))
    conn.commit()
    return cursor.rowcount


if __name__ == "__main__":
    import sys, pathlib
    # Ensure project root is on sys.path so package imports work
    sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))
    conn = connect_database()
    load_all_csv_data(conn)
    conn.close()