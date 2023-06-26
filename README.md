# Options to Implement Record Retention Policy
This repository proposes multiple options for organisations to delete old data in their systems using older and some newer mechanisms.

This was created in response to customer questions asking for the "best way to delete old data" in Databricks.

## TODOs
1. Add a table with Slowly Changing Dimensions (person_info_history) and validate each approach against it.
2. Build the streaming option which will utilise delete flags on incoming data.
3. Switch to logger solution: https://medium.com/@dlevy_18530/better-logging-in-databricks-e7f6e14eb24d

## Hard Requirement
The key requirement we're looking to implement is to deny most users being able to be used in queries after a specified retention of data.

## Solution Dependent Differences
Though the solutions all satisfy the main requirement, there will be a few key differentiators between them:

1. Is it a hard delete or a soft delete?
2. How complicated is the process?
3. Does it limit system use in certain ways?
4. Is it streaming compatible?
5. Does it depend on a Delete Flag?
6. Is it compatible with Slowly Changing Dimensions Type 2 (SCD2) or Data Vault?

## Out of Scope
We haven't looked at a process to hunt-down user generated tables (derivative data products) but have added thoughts on this in the Final Reflections.

## Cluster Pre-requisites
* Databricks Runtime: These scripts were tested on DBR 13 with Unity Catalog.
* Faker: If you want to avoid installing the Python faker library, you should install as a cluster dependency. 
* Row Level Filters: If you want to run the Row Level Filter solution, you will need to talk to your Databricks Account team to be enrolled in the preview.

## Solutions
We provide the following solutions:

* [Option 1 - Delete By Date]($./Option 1 - Delete By Date) - Delete data within Databricks Lakehouse using a date predicate
* Option 2 - Delete Flags on incoming data (TODO) - This will be the standard MERGE with a Delta style approach.
* [Option 3 - Views]($./Option 3 - Create a View) - Create views which hide data
* [Option 4 - Row Level Filter]($./Option 4 - Row Level Filter) - Use Row-Level Filters which hide data

## Comparison
We have built the following table to compare the solutions. Complexity is subjective - I've applied a general rule that if there's something you need to "do" everytime you create a new table than I've increased the complexity. Row Level Filters get a pass as they're simple to apply and can be re-used, whereas Views require ACLs and an additional DDL statement.

|                       | *Option 1 - Delete Data* | *Option 2 - Delete on Merge* | *Option 3 - Create Views* | *Option 4 - Row Level Filters* |
| --------------------- | ---------------------- | -------------------------- | ----------------------- | ---------------------------- |
| Hard or Soft Delete   | Hard                   | Hard                       | Soft                    | Soft                         |
| Complexity?           | Easy                   | Average                    | Average                 | Easy                         |
| Limitations           | Time Travel            | Time Travel                | Time Travel             | None                         |
| Streaming Compatible? | No                     | Yes                        | Yes                     | Yes                          |
| Upstream Dependent    | No                     | Yes                        | No                      | No                           |
| SCD2 / Data Vault Compatible? | No                     | No                        | Yes                     | Yes                          |


## Recommendations
I believe that Row Level Filters are the best method for most deletions, which can be combined with deleting the data from option 1. This hybrid approach gives the best of both worlds:

* You can run your deletion processes less frequently, maybe on a weekend or end-of-month and still be policy compliant.
* You still can benefit from Time Travel as you don't need to run a mandatory VACUUM and can leave older versions of the table until it is convenient to delete.

## Final Reflections
Doing this exercise was a good chance to see how various approaches actually stack up and hopefully will help organisations take a consistent approach.

The work here could be further utilised to build a "crawler" which uses Unity Catalog table/column metadata as well as lineage to discover downstream "user generated" tables that may hold data assets beyond retention periods. Though out of scope for this exercise, it would be very interesting to build something to do this to "cleanup" old data in a system.

