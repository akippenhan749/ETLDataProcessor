# ETL Data Processor

</a>Usage
------------------------------------------

This ETL data processor can be run by entering the following in a terminal environment:

`python ETLDataProcessor.py <data source> <output format>`

`<data source>` is the filename or url of the data source to read from

`<output format>` is the desired output format and must be one of `CSV`, `JSON`, `SQL` or `XLSX`

This program uses the following modules:

`OS` - for filename reading,

`Pandas` - for data manipulation,

`SQLAlchemy` - for converting to a SQL database

`SYS` - for command line arguments.

</a>Description
------------------------------------------

This ETL data processor has the ability to take in a source data file in a variety of formats and convert it into the requested format. After checking that the user inputted the command line arguments correctly and the passed data source file exists, it loads the source data into a Pandas dataframe. From this point, if the user requested to convert the data to a SQL database, the data processor creates a new MySQL database and table within within it with the same name as the source data file with 'Db' and 'Tbl' appended to the ends of names of the database and table within it. If the user requested an output format to `CSV`, `JSON`, or `XLSX`, the data processor calls the appropriate Pandas function to convert to the requested format. The filename of the converted file will be the same as the data source file with the exception of the file extension. Once the conversion is finished, the program will also output a breief summary of the converted data file with a preview of the data, containing the first five rows and the number of records and columns. Two different datasets in various formats can be found in the data folder of this repository.

This project was created as part of the Data Science Systems (DS 3002) course at the University of Virginia in the Spring of 2022.
