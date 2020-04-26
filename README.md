# 5300-Ladybug
DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2020


<h2>Milestone 1: Skeleton</h2>
Parts pertaining to this milestone reside in sql5300.cpp, mySQLParser.h and mySQLParser.cpp

sql5300.cpp - driver for the program that initializes the environment and starts loop 
that accepts inputs and returns if they are SQL statements or not (limited functionality). 
If you write quit, then the program ends. When you run it you need to specify a path to 
where you wish to run the database environment like: 
$ ./sql5300 cpsc5300/data

mySQLParser.h and mySQLParser.cpp - namespace of static functions that the driver uses to
evaluate SQL statements. First, the input goes into the sqlStatementToString method where
the statements is put into a switch statement and then sent to either createStatementToString 
or selectStatementToString functions. Ther they get turned into strings, utilizing all the other
methods of the namespace based on what statement they are processing. All the methods are commented in cpp file.


<h2>Milestone 2: Rudimentary Storage Engine</h2>
Parts pertaining to this milestone reside in storage_engine.h, heap_storage.h and heap_storage.cpp
although sql5300.cpp was modified so that if you write test, it runs a test program from heap_storage.cpp

storage_engine.h - Contains base classes for database blocks, files and relations and plenty of other datatypes
or datatype redefinitions that are used throughout heap_storage files

heap_storage.h - Containes classes that extend storage_engine classes: SlottedPage (extend DbBlock), 
HeapFile(DbFile), and HeapTable (DbRelation). Essentially the hierarchy is: 
HeapTable > HeapFile > SlottedPage
With HeapTable using one Heapfile and Heapfile accessing multiple slottedpages for holding records

heap_storage.cpp - At the top are some test functions for the rest of the methods. Below, the methods are
separated by comments into the three different classes. Slottedpage and Heapfile are fully implemented whilst
HeapTable still doesn't support update, delete and project with column name specifications.



Video: [link to video on Sprint 1](https://www.youtube.com/watch?v=MABRjxSOglM&feature=youtu.be)

