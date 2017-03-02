# SensorThingsImporter
A tool for importing observations from a CSV file into a SensorThings API compatible service.

A Gui mode is planned but not implemented yet.

Command line options are described in the help output:
```
java -jar SensorThingsImporter-0.1-SNAPSHOT.jar -help

-charset -c [character set] :
    The character set to use when parsing the csv file.

-file -f [file path] :
    The path to the csv file.

-server -s [url] :
    The url of the server.

-resultcol -rc [column nr] :
    The column # that holds the result (first is 0).

-phentimecol -ptc [column nr] :
    The column # that holds the phenomenonTime (first is 0).

-validtimecol -vtc [column nr] :
    The column # that holds the validTime (first is 0).

-resulttimecol -rtc [column nr] :
    The column # that holds the resultTime (first is 0).

-datastream -d [datastream Id] :
    The datastream id to add the observations to.

-datastreamfilter -dsf [filter] :
    A filter that will be added to the query for the datastream.
    Use placeholders {colNr} to add the content of columns to the query.
    Example: -dsf 'Thing/properties/id eq {1}'

-rowskip -rs [row count] :
    The number of rows to skip when reading the file (0=none).

-rowlimit -rl [row count] :
    The maximum number of rows to insert as observations (0=no limit).

-sleep [duration] :
    Sleep for this number of ms after each insert.

-noact -n :
    Read the file and give output, but do not actually post observations.

-tab :
    Use tab as delimiter instead of comma.

-basic [username] [password] :
    Use basic auth.

-messageinterval -mi [interval] :
    Output a progress message every [interval] records. Defaults to 10000

```