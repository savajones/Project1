# Description
Project 1 is an analysis of the 2019 NFL Combine using a Scala console application that is retrieving data using Hive.
# Features
- Login functionality
  - 2 types of users: ADMIN & BASIC
  - Partitioned table
- Analysis:
  - Average 40yd,Vertical,Bench,Broad Jump,3Cone,Shuttle
    - Overall & by position
  - Top 10 athletes in each event
  - Min and max of each event
  - Number of different schools that got drafted
  - School with the most draft picks
  - Clemson athlete draft picks (tm/rnd/yr)
    - ADMIN can change school and export into JSON file
# Technologies
- YARN
- HDFS
- Scala
- Hive
- Hadoop
# Get Started
$ git clone https://github.com/savajones/Project1
# Usage
Enter a numerical value from the provided menu:

![Screen Shot 2022-04-05 at 12 04 52 PM](https://user-images.githubusercontent.com/100616163/161797755-e5a199c8-0161-4bda-973e-22ebe402e2fc.png)
![Screen Shot 2022-04-05 at 12 06 05 PM](https://user-images.githubusercontent.com/100616163/161797768-800c8ee8-06ed-41c8-80b5-7f9a52328169.png)
# Future Development
- Implement bucketing
- Export all queries to JSON
- Encrypted passwords for login
- Handle input mismatch
# Data Resource
https://www.pro-football-reference.com/draft/2019-combine.htm#combine
