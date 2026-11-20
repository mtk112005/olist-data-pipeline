-- Runs once when the Postgres volume is first created.
-- One server, two databases: Airflow metadata and the analytics warehouse.
create user airflow with password 'airflow';
create database airflow owner airflow;

\connect warehouse
create schema if not exists raw authorization olist;
