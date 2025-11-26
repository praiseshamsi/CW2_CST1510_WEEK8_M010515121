## WEEK8:Data Pipeline & CRUD (SQL)
Student Name: [PRAISE SHAMSI AYEBAZIBWE] Student ID: [M01051521]
Course: CST_CW2_Multi_Domain_Intelligence Platform

## Project description 
Transitioning from file-based storage to a professional database system (SQLite).
This week focuses on creating tables, migrating data, loading CSV files, and building secure CRUD operations for a multi-domain intelligence platform.

## Features

Creation of all database tables
Migration of user accounts from users.txt
CSV loading into multiple domain tables
Table row validation to confirm everything imported correctly

## Technical Implementation
- Database system: SQLite, managed with pathlib.Path
- Python modules: `sqlite3, `bcrypt`, `pandas`, `pathlib`
- Security measures: Input validation enforced, Parameterized queries used, Passwords stored securely as bcrypt hashes, Duplicate usernames prevented
- CRUD operations: Python functions for creating, reading, updating, and deleting database records
- User migration: Week 7 `users.txt` converted into database rows
