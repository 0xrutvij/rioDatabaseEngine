# Rudimentary Database Engine
## (DavisBase: Rio de Janeiro)

<br/>

### (1) Environment
- Create a virtual environment with Python 3.9.7+ within a clone of this repo
    
    ```bash
    git clone https://github.com/0xrutvij/rioDatabaseEngine.git

    cd rioDatabaseEngine
    
    # if using virtualenv
    virtualenv venv
    
    # else
    python3 -m venv venv

    # then source it (*nix OSes)
    source venv/bin/activate

    # install all dependencies
    pip install -r requirements.txt 
    ```

### (2) Launching the Application

- Launching the SQL REPL 

    ```bash
    python3 src/main_loop.py
    ```
    
    The output will be something like this!

    ```bash
    -------------------------------------------
    Welcome to RioDBLite
    RioDBLite Version 0.1
    ©2021 Rio DB Group
    
    Type "help;" to display supported commands.
    -------------------------------------------
    

    riodb>
    ```

### (3) How to Start

- run init db command only the first to initialize the database and create database files, it will create a file named
rio.db in the current directory, and supports one database instance per directory.

- Look at the commands supported and sample commands for next steps.

    ```sql
    riodb> init db;
    ```

- Subsequent launches should read all tables present in the `rio.db` file.

- The immediately previous version is backed up at all times to `rio.db.bkp`, to restore from backup, rename the bkp file
to `rio.db` before next launch and remove any other files of the same name. 


### (4) Commands Supported (w/ Sample Commands)

- clear: Clear previous input data `clear;`

- create: create a new table in the database

    ```sql
    CREATE TABLE DOGS 
    (
        TagID int PRIMARY KEY, 
        Name text, Weight float, 
        Age int
    );
    ```

- insert: Insert data into a particular table 
    
    ```sql
    INSERT INTO TABLE
    (TagID, Name, Weight, Age) 
    DOGS 
    VALUES (933, "Rover", 20.6, 4);
    ```
    
- select: Query data from the database
    
    ```sql
    SELECT * FROM DOGS;
    ```

- update: Update data in the tables

    ```sql
    UPDATE DOGS SET Age=8 WHERE Name=“Rover”;
    ```
- delete: Delete data from a table
    
    ```sql
    DELETE FROM TABLE DOGS WHERE Name=“Rover”;
    ```
- drop: Delete table from database

    ```sql
    DROP TABLE dogs;
    ```

- exit: Exit davisbase RioDB `exit;`






