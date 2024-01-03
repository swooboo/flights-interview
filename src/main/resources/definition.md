Spark exercise

Inputs:
1. Flights -
   https://drive.google.com/file/d/127ZPqt75nZu_XQEHhmTbXtuaxLUUY3gm/view?usp=drive_link
2. Airports -
   https://drive.google.com/file/d/11wN6K8MVH3lYWTDLYdta7dMVbTMmD6w0/view?usp=drive_l
   ink
   Task details:
1. Explore the data and load the files into Dataframes with match schema according the following
   configurations:
   a. Unified fields names
   b. Data types
2. Find the flight date according the following logic:
   a. Each flight contains fields of day of week & day of month
   b. You should fill the latest date (before 1/12/2023( that match the day of week and day of
   month. i.e.: if day of week is 4 and day of month is 1 the result should be 01/11/2023
   (it’s Wednesday → day of week is 4, it’s the 01 → day of month is 1)

3. Perform the following queries:
   a. Calculate the number of flights grouped by date, carrier and origin airport. The result
   needs to show top 20 results ordered by number of flights descending and show the
   airport name.
   b. Show most common tracks (carrier, origin airport, destination airport)