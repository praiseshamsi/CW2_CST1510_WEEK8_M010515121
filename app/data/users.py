from app.data.db import connect_database

def get_user_by_username(username):
    """Retrieve user by username."""
    conn = connect_database()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM users WHERE username = ?",
        (username,)
    )
    user = cursor.fetchone()
    conn.close()
    return user

def insert_user(username, password_hash, role='user'):
    """Insert new user."""
    conn = connect_database()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
        (username, password_hash, role)
    )
    conn.commit()
    conn.close()


import pandas as pd
import bcrypt

def register_user(conn, username, password, role='user'):
    """
    Register a new user with hashed password.

    Args:
        conn: sqlite3.Connection object
        username: str, unique username
        password: str, user password
        role: str, optional user role, default 'user'

    Returns:
        int: ID of the inserted user
    """
    cursor = conn.cursor()
    password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    sql = "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)"
    cursor.execute(sql, (username, password_hash, role))
    conn.commit()
    return cursor.lastrowid


def get_all_users(conn):
    """
    Retrieve all users from the database.

    Args:
        conn: sqlite3.Connection object

    Returns:
        pd.DataFrame: DataFrame containing all users
    """
    df = pd.read_sql_query("SELECT id, username, role, created_at FROM users", conn)
    return df


def update_user_role(conn, username, new_role):
    """
    Update the role of a user.

    Args:
        conn: sqlite3.Connection object
        username: str, username of the user to update
        new_role: str, new role

    Returns:
        int: Number of rows updated
    """
    cursor = conn.cursor()
    sql = "UPDATE users SET role = ? WHERE username = ?"
    cursor.execute(sql, (new_role, username))
    conn.commit()
    return cursor.rowcount


def delete_user(conn, username):
    """
    Delete a user from the database.

    Args:
        conn: sqlite3.Connection object
        username: str, username to delete

    Returns:
        int: Number of rows deleted
    """
    cursor = conn.cursor()
    sql = "DELETE FROM users WHERE username = ?"
    cursor.execute(sql, (username,))
    conn.commit()
    return cursor.rowcount
