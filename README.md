# UnixIdAutoSet

A simple program to assign UNIX ID numbers to objects in a Active Directory with RFC 2307 atributes.

Membership of a group is used to recognize which users and groups need to have their `uidNumber` and `gidNumber` attribute set and a SQLite database is used to remember this.

The IDs are given monotonically from another table in the same SQLite database, so removing objects do not affect the sequence.


## Dependencies

The program is written in .NET Framework 4.5 and additionally depends on `System.Data.SQLite.Core`.

The usage of `System.DirectoryServices` makes this program Windows-only for now.


## Usage

Simply running the program does the changes in the Active Directory using the current credentials, the `dryrun` argument can be passed to check which changes are made.

