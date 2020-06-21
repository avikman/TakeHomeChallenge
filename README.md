# TakeHomeChallenge

## How to run ?


1) Please install Postgres database into docker using below steps :
 - Open docker terminal.
 - run `docker pull postgres:11.5`
 - run `docker run --name [container_name] -e POSTGRES_PASSWORD=[your_password] -d postgres`
 - verify whether postgres container is created by running `docker ps`
 - login into postgres database, run `psql -U postgres`
 - check existing databases, run `\l`
 - create new database by running `CREATE DATABASE [database_name]` or take existing database for this project

2) Please download the project from gitHub and import it into Intellij.

3) Download scala plugin for Intellij and add scala Framework Support for the imported project.

4) Download source file from specified link - https://www.kaggle.com/karangadiya/fifa19/download

5) Open SolutionUsingDataFrame.scala in IntelliJ and specify below attributes as per your setup.
- source file path
- postgres database connection url and propertry file details
- target table name into postgres database

6) Execute SolutionUsingDataFrame.scala

7) Output for Step 2 is available into `Run` section of IntelliJ.

8) Postgres table data can be checked from shell using below steps
- login into postgres database, run `psql -U postgres`
- use particular database, run `\c [database_name]`
- verify whether table is created, run `\dt`
- check table description, run `\d [table_name]`
- check record count, run `select count(1) from [table_name]`. Right now output will be 18207.
- check table data, run `select * from [table_name] limit 5`

