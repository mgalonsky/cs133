/* Remove tables if they are already here */
drop table if exists Person;
drop table if exists Car;
drop table if exists Trip;
drop table if exists Passenger;

/* Turn on some useful features: foreign constraint checking, and pretty-printing */
.header on
.mode column
PRAGMA foreign_keys = ON; -- turns on checking for foreign keys constraints

CREATE TABLE Person (id INTEGER, sname CHAR(20), phone_number CHAR(10), PRIMARY KEY (id));
CREATE TABLE Car (license_plate CHAR(7), id INTEGER, make_and_model CHAR(20), capacity INTEGER, PRIMARY KEY(license_plate), FOREIGN KEY(id) REFERENCES Person);
CREATE TABLE Trip (date CHAR(15), time CHAR(5), direction CHAR(6), id INTEGER, license_plate CHAR(7), PRIMARY KEY(date, direction, license_plate), FOREIGN KEY(license_plate) REFERENCES Car);
CREATE TABLE Passenger (id INTEGER, date CHAR(15), direction CHAR(6), license_plate CHAR(7), PRIMARY KEY(id, date, direction), FOREIGN KEY(id) REFERENCES Person, FOREIGN KEY(date, direction, license_plate) REFERENCES trip);