-- Create
CREATE TABLE DOGS (TagID int PRIMARY KEY, Name text, Weight float, Age int);

-- Show tables

show tables;

-- Insert
INSERT INTO TABLE(TagID,Name,Weight,Age) DOGS VALUES(933,"Rover",20.6,4);
INSERT INTO TABLE(TagID,Name,Weight,Age) DOGS VALUES(8326,"Spot",10.8,7);
INSERT INTO TABLE(TagID,Name,Weight,Age) DOGS VALUES(5359,"Lucky",31.2,5);
INSERT INTO TABLE(TagID,Name,Weight,Age) DOGS VALUES(10355,"Dinky",4.8,11);
INSERT INTO TABLE(TagID,Name,Weight,Age) DOGS VALUES(7757,"Bruiser",42.0,6);
INSERT INTO TABLE(TagID,Name,Weight,Age) DOGS VALUES(3597,"Patch",29.6,9);
INSERT INTO TABLE(TagID,Name,Weight,Age) DOGS VALUES(202,"Prince",16.6,7);
INSERT INTO TABLE(TagID,Name,Weight,Age) DOGS VALUES(1630,"Bubbles",7.1,1 );
INSERT INTO TABLE(TagID,Name,Weight,Age) DOGS VALUES(11223,"Peanut",14.3,2 );

-- Delete Where

Delete from TABLE dogs where Weight <= 11;

-- Select * X *

Select * from dogs;

-- Select * where

Select * from dogs where TagID = 11223;

-- Update Where

Update dogs set Weight = 12 where TagID = 11223;

-- Select * where

Select * from dogs where TagID = 11223;

-- Drop Table

Drop table dogs;

-- Show tables

show tables;
