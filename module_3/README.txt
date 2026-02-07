Needed to run: 
brew install postgresql
brew services start postgresql
initdb /usr/local/var/postgresql@14
Did the following: 
psql -d postgres
postgres=# CREATE DATABASE gregmiller;
CREATE DATABASE
postgres=# CREATE ROLE postgres WITH LOGIN SUPERUSER PASSWORD 'abc123';
CREATE ROLE
