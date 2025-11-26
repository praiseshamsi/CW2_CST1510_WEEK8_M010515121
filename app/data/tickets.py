import pandas as pd
from app.data.db import connect_database

def insert_ticket(conn, ticket_id, priority, status, category, subject,
                  description=None, created_date=None, resolved_date=None,
                  assigned_to=None):
    """
    Insert a new IT ticket into the database.

    Args:
        conn: sqlite3.Connection object
        ticket_id: str, unique ticket ID
        priority: str, priority level ('High', 'Medium', 'Low')
        status: str, ticket status
        category: str, ticket category
        subject: str, ticket subject
        description: str, optional ticket description
        created_date: str, optional creation date
        resolved_date: str, optional resolved date
        assigned_to: str, optional assignee

    Returns:
        int: ID of the inserted ticket
    """
    cursor = conn.cursor()
    sql = """
    INSERT INTO it_tickets
    (ticket_id, priority, status, category, subject, description,
     created_date, resolved_date, assigned_to)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(sql, (ticket_id, priority, status, category, subject,
                         description, created_date, resolved_date, assigned_to))
    conn.commit()
    return cursor.lastrowid

def get_all_tickets(conn):
    """
    Retrieve all tickets from the database.

    Args:
        conn: sqlite3.Connection object

    Returns:
        pd.DataFrame: DataFrame containing all tickets
    """
    df = pd.read_sql_query("SELECT * FROM it_tickets", conn)
    return df

def update_ticket_status(conn, ticket_id, new_status):
    """
    Update the status of a ticket.

    Args:
        conn: sqlite3.Connection object
        ticket_id: str, ticket ID
        new_status: str, new status

    Returns:
        int: Number of rows updated
    """
    cursor = conn.cursor()
    sql = "UPDATE it_tickets SET status = ? WHERE ticket_id = ?"
    cursor.execute(sql, (new_status, ticket_id))
    conn.commit()
    return cursor.rowcount

def delete_ticket(conn, ticket_id):
    """
    Delete a ticket from the database.

    Args:
        conn: sqlite3.Connection object
        ticket_id: str, ticket ID to delete

    Returns:
        int: Number of rows deleted
    """
    cursor = conn.cursor()
    sql = "DELETE FROM it_tickets WHERE ticket_id = ?"
    cursor.execute(sql, (ticket_id,))
    conn.commit()
    return cursor.rowcount
