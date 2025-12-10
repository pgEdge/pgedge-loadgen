# Project Design Notes

## Overview

pgedge-loadgen is a command line tool written in Go. It is designed to connect
to a PostgreSQL database, and create and populate a schema for one of a 
selection of fictional applications, based on those used by various TPC
benchmarks and more.

When the user initialises a new database, a schema is created based on the 
selected application, and populated with generated test data sized based on
parameter that will roughly define the resulting size of the PostgreSQL
database on disk. The user should be able to specify a size such as 5GB,
and the resulting generated database should be approprimately that size 
excluding WAL and assuming no bloat.

Once a database is initialised as required, the user will be able to run the
application again, specifying the maximum number of client connections. The
application will connect, and run queries simulating normal users of the 
application, including OLTP, mixed, and OLAP queries as appropriate for the
type of application being simulated.

THIS IS NOT A BENCHMARK APPLICATION. The queries should be executed using 
timing that emulates the actual usage that one might expect from such an 
application, taking into account seasonality such as time of day, week, month
and so on. The user should be able to select from usage profiles such as 
local office (where usable might be roughly 8AM - 6PM, with dips for lunch
and breaks, and some overnight batch processing), usage in a global company
where break times might vary, and "overnight" equates to a much smaller time 
window, or usage of a public facing regional or global application such as an
online store where users might be active in the evenings.

The application should be designed to run continuously for extremely long 
periods of time, without crashing or requiring maintenance besides typical
automatic log rotation and PostgreSQL's autovacuum/analyse.