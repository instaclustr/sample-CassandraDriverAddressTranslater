# sample-CassandraDriverAddressTranslater

## Notes

The example was tested on Cassandra 2.2.5 using the Datastax driver in version 2.x, with SSL and password authentication enabled.

Minor modifications are required on Cassandra 3.x as the queried keyspace.table has a slightly different name.

Minor modifications are also required if using Datastax driver in version 3.x as the AddressTranslater interface has been renamed  AddressTranslator, and is having additional methods to implement.



[The Main source file is here](src/main/java/com/company/Main.java)

Please see https://www.instaclustr.com/support/documentation/announcements/instaclustr-open-source-project-status/ for Instaclustr support status of this project
