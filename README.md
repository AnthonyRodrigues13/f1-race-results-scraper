F1 Race Results Scraper

This project automates the process of extracting Formula 1 race results from Pitwall and storing them into a MySQL database.
It dynamically fetches race, qualifying, and practice session data across multiple seasons and saves them as structured tables for easy access and analysis.

Features
  - Connects to a MySQL database using mysql.connector and SQLAlchemy.
  - Scrapes F1 race, qualifying, and practice session data from Pitwall for seasons ≤ 2006 and beyond.
  - Dynamically creates tables in MySQL for each race, named by season and race name.
  - Uses pandas.read_html() to efficiently parse race data from web pages.
  - Handles chunked data insertion for scalability and performance.

Tech Stack
  - Python 3
  - BeautifulSoup
  - pandas
  - MySQL Connector
  - SQLAlchemy
