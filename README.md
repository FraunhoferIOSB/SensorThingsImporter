# SensorThingsImporter
A tool for importing observations from a CSV file into a SensorThings API compatible service.

Starting without parameters opens the gui, which can be used to create or edit a configuration file.

Command line options are described in the help output:
```
java -jar SensorThingsImporter-0.1-SNAPSHOT.jar -help

-noact -n :
    Read the file and give output, but do not actually post observations.

-config -c [file path] :
    The path to the config json file.
```
