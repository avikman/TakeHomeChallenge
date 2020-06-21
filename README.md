# TakeHomeChallenge

## How to execute ?


1) Please install Postgres database into docker using below steps:
   - Open docker terminal.
   - run `docker pull postgres:11.5`.
   - run `docker run --name [container_name] -e POSTGRES_PASSWORD=[your_password] -d postgres`.
   - verify whether postgres container is created, run `docker ps`.
   - login into postgres database, run `psql -U postgres`.
   - check existing database list, run `\l`.
   - create new database by running `CREATE DATABASE [database_name]` or take existing database for this project.

2) Please download the project from gitHub and import it into Intellij.

3) Download scala plugin for Intellij and add scala Framework Support for the imported project.

4) Download source file from https://www.kaggle.com/karangadiya/fifa19/download and unzip it.

5) Open SolutionUsingDataFrame.scala in IntelliJ and specify below attributes as per your setup.
   - source file path along with filename
   - postgres database connection url and propertry file details
   - target table name into postgres database

6) Execute `SolutionUsingDataFrame.scala` from IntelliJ.

7) Output for Step 2 is available into `Run` section of IntelliJ.

8) Postgres table data can be checked from shell using below steps
   - login into postgres database, run `psql -U postgres`
   - use particular database, run `\c [database_name]`
   - verify whether table is created, run `\dt`
   - check table description, run `\d [table_name]`
   - check record count, run `select count(1) from [table_name]`. Right now output will be 18207.
   - check table data, run `select * from [table_name] limit 5`

## Output

Output of **step 2** received from IntelliJ
![Output from IntelliJ](https://github.com/avikman/TakeHomeChallenge/blob/master/snaps/intelliJ.png?raw=true)

Verification done for **step3** from PostgresSQL shell
![Output from PostgresSQL shell](https://github.com/avikman/TakeHomeChallenge/blob/master/snaps/postgresdb.png?raw=true)


## My approach

For **step 1**, I have read data from csv file and written into parquet file after extracting digits from `Value` and `Wage` column and convert it into thousand pound currency. So that no need to perform repeated transformation for `Wage` and `Value` in step 2 & 3. By using parquet file format, faster performance is achived in step 2. To accomodate daily delta file, SCD type 1 will be applied.

For **step 2**, spark dataframe is created by reading parquet file and persist it till execution of step2.
 - for *problem a*, at first players who is below 30 years old left footed Midfielders, is filtered from all clubs (Assumption is: Midfielders positions are RWM RM RCM CM CAM CDM LCM LM LWM ) and count number of players it with respect to `Club`. Then select the clubs which contains most number of players. 
 - for *problem b*, at first players playing into positions of 4-4-2 formation are filtered from all clubs, then applied window functions to calculate best players for particular position from a club. then picked the best players for each club and calculate the average `Overall` for the clubs to select the strongest club for 4-4-2.
 - for *problem c*, applied summation of player's `Value` & `Wage` for a club, to get clubs value and spent wage bill respectively. Then selected the club having most value and spending maximum wage bill.
 - for *problem d*, calculated average of wage for each position and then selected the position having maximum value of average wage.
 - for *problem e*, calculated average skills for good players, playing at goalkeeper `Position` (GK). Then transposed the average skill output to select top 4 average valued skills.
 - for *problem f*, calculated average skills for good players, playing at striker `Position` (ST). Then transposed the average skill output to select top 4 average valued skills. Assumption for *problem e* & *problem f* is good players have gold card, that means their `Overall` rating is greater than equals to 74.   
 
 For **step 3**, read the data from output parquet file created in step 1 and renamed the required column names and applied logic to read date from `MMM dd,yyyy` format, before loading the data into postgres database.

