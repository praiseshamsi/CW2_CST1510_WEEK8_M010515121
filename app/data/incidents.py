import pandas as pd
import sqlite3


def insert_incident(conn: sqlite3.Connection, date: str, incident_type: str, severity: str,
                    status: str, description: str, reported_by: str = None) -> int:
    """
    Insert a new cyber incident into the database.

    Args:
        conn: Database connection
        date: Incident date (YYYY-MM-DD)
        incident_type: Type of incident
        severity: Severity level
        status: Current status
        description: Incident description
        reported_by: Username of reporter (optional)

    Returns:
        int: ID of the inserted incident
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
    """Retrieve all incidents from the database as a DataFrame."""
    df = pd.read_sql_query("SELECT * FROM cyber_incidents ORDER BY id DESC", conn)
    return df


def update_incident_status(conn: sqlite3.Connection, incident_id: int, new_status: str) -> int:
    """Update the status of an incident."""
    cursor = conn.cursor()
    sql = "UPDATE cyber_incidents SET status = ? WHERE id = ?"
    cursor.execute(sql, (new_status, incident_id))
    conn.commit()
    return cursor.rowcount


def delete_incident(conn: sqlite3.Connection, incident_id: int) -> int:
    """Delete an incident from the database."""
    cursor = conn.cursor()
    sql = "DELETE FROM cyber_incidents WHERE id = ?"
    cursor.execute(sql, (incident_id,))
    conn.commit()
    return cursor.rowcount




