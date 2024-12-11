## minidb Overview

minidb is implemented as a lightweight database management system in C++. Here is a brief overview of its design:

### Database and Table Management

The `Database` class manages a collection of tables, each represented by the `Table` class. Tables contain columns (`Column` struct) and records (`Record` struct).

### Commands

It supports basic database commands such as creating a database (`createDatabase`), using a database (`useDatabase`), creating a table (`createTable`), inserting records (`insertIntoTable`), selecting records (`select`), and dropping tables (`dropTable`).

### File Operations

Databases can be saved to and loaded from files, ensuring persistence across sessions.

---

## File Summary

### data_manager.cpp

Implements core functionalities of the `MiniDB` class, such as creating databases, using databases, creating tables, inserting records, selecting records, and saving/loading the database to/from files.

### query_executor.cpp

Failed to retrieve the file content.

### main.cpp

Entry point for the application. It reads SQL commands from an input file, processes them using the `MiniDB` instance, and writes the output to a CSV file.

### query_executor.hpp

Failed to retrieve the file content.

### data_manager.hpp

Defines the structures and classes used in the `MiniDB` project, including `Column`, `Record`, `Table`, `Database`, and `MiniDB`. It declares the interface for the `MiniDB` class.

### removespace.cpp

Implements the `trim` function used to remove whitespace from the beginning and end of a string.

### removespace.hpp

Declares the `trim` function used to remove whitespace from the beginning and end of a string.
