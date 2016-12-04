# SQLCell
Run raw SQL in an IPython Notebook without a wrapper. SQLCell Readme with IPython Notebook Markdown can be found <a href="https://cdn.rawgit.com/tmthyjames/2928d9df36b451bcf9f10f1e21f0dae8/raw/1fc4266bab4e61dd3ad37fde29a0d5e48783b57b/SQLCell_Readme.html"><b>here</b></a>

![SQLCell preview](images/initial_query_lg.png)

	    
## Contents
&emsp;<br/>• [Most recent feature](#most-recent-feature)
&emsp;<br/>• [Installation and Configuration](#installation-and-configuration)
&emsp;<br/>• [Features](#features)
<br/>&emsp;i. [Parameters](#parameters)
<br/>&emsp;ii. [Pass python variables to SQL](#pass-python-variables-to-sql)
<br/>&emsp;iii. [`psql` metacommands](#psql-metacommands)
<br/>&emsp;iv. [Multi-threading](#multi-threading)
<br/>&emsp;v. [Buttons](#buttons)
<br/>&emsp;vi. [Inline editing](#inline-editing)
<br/>&emsp;vii. [Easy-to-read Query Plan Table](#easy-to-read-query-plan-table)
<br/>&emsp;viii. [Easy-to-read Query Plan Graph](#easy-to-read-query-plan-graph)
<br/>&emsp;ix. [Switch Engines](#switch-engines) (To be documented...)
&emsp;<br/>• [Pipeline](#pipeline)
<br/>&emsp;i. [Easy-to-read Query Plan Graph](#easy-to-read-query-plan-graph) (Added)
&emsp;<br/>• To dos
<br/>&emsp;i. Add UI elements to perform basic, common database tasks, such as adding columns, creating tables, etc.

Open issues can be found <a href="https://github.com/tmthyjames/SQLCell/issues">here</a>.


##Most recent feature

added 12/04/2016

SQLCell now offers the option to view the Query Plan as a sankey graph built with D3.js

![query_plan_sankey](images/query_plan_sankey.png)


##Installation and configuration

Just clone the repo and `cp` the `sqlcell_app.py` file to Jupyter's startup directory (on my computer, the directory is `~/.ipython/profile_default/startup`, but may be different depending on your OS and version of IPython/Jupyter):

    $ cd .ipython/profile_default/startup # or wherever your startup directory is
    $ git clone https://github.com/tmthyjames/SQLCell.git
    $ cp SQLCell/sqlcell_app.py sqlcell_app.py # place sqlcell_app.py in the startup folder so it will get executed
    
Then in the engine_config.py file, define your connection variables. If you don't add them to engine_config.py, 
then you'll have to pass a connection string to the ENGINE parameter everytime you use `%%sql`, like so:

	In [2]: %%sql ENGINE='postgresql://username:password@host:port/database'
	        SELECT * FROM table;
    
Now you are ready to ditch pgAdmin or whatever SQL interface you use. Continue reading to see all the available 
options, like writing results to a CSV, using SQLAlchemy named parameters and more.

##Features

###Parameters

Available parameters:
<br/>• [`DB`](#db-parameter): Determines what database to query. On the first run, this parameter is required. After that, it will remember what database was chosen. To change databases, use this parameter again. Default is the last-specificed database.
<br/>• [`PATH`](#path-parameter): Writes results of the query to a CSV (can also be done through the UI). No default.
<br/>• [`MAKE_GLOBAL`](#make_global-parameter): Passes results of the query to the variable you pass to it. If this parameter is specified but the `RAW` parameter is not, then the results will be a Pandas DataFrame. If `RAW` is set to `True`, then the results will be the raw RowProxy returned from the database. No Default.
<br/>• [`RAW`](#raw-parameter): Determines whether the data will be of type DataFrame or RowProxy. Default: `False`.
<br/>• [`DISPLAY`](#display-parameter): Determines whether or not to render the results as a table. This is best used in conjunction with the `MAKE_GLOBAL` parameter because displaying a table in a busy workflow can be cumbersome and annoying sometimes.
<br/>• [`ENGINE`](#engine-parameter): Speicifies what host, database to connect to. Default is the connection that is specified in the engine_config.py file. If the engine_config.py file is not configured, then the `ENGINE` parameter is required.
<br/>• [`TRANSACTION_BLOCK`](#transaction_block-parameter)': Determines whether the query will be executed inside a transaction block or not. This is useful when creating a database, dropping a database, `VACUUM ANALYZE`ing a database, or any other query statements that cannot be run inside a transaction block. Default: True
<br/>• [`EDIT`](#edit-parameter): Enables inline editing. To use this, you must specify only one table in your query, and that table must have a primary key. Default: False.



Examples of how to use these are below.

####`DB` Parameter
After adding your connection details to engines.py, run your first query with the DB argument:

	In [3]: %%sql DB=bls
	        SELECT * 
	        FROM la_unemployment
	        LIMIT 3
<table class="table-striped table-hover" id="table18a296f1-580e-4c02-b042-6e9279445a68" width="100%"><thead><tr><th> </th><th>series_id</th><th>year</th><th>period</th><th>value</th><th>footnote_codes</th></tr></thead><tbody><tr><td>1</td><td>LASST470000000000003</td><td>1976</td><td>M01</td><td>6.2</td><td>None</td></tr><tr><td>2</td><td>LASST470000000000003</td><td>1976</td><td>M02</td><td>6.1</td><td>None</td></tr><tr><td>3</td><td>LASST470000000000003</td><td>1976</td><td>M03</td><td>6.0</td><td>None</td></tr></tbody></table>


For the rest of the session, you won't have to use the DB argument unless you want to change
databases. And the last-used DB will be persisted even after you shut down Jupyter and start it back up next time. 

	In [4]: %%sql
	        SELECT * 
	        FROM avg_price LIMIT 3
<table class="table-striped table-hover" id="table092a65ed-f041-472d-8a5a-2cbc79c7df53" width="100%"><thead><tr><th> </th><th>series_id</th><th>year</th><th>period</th><th>value</th></tr></thead><tbody><tr><td>1</td><td>APU0000701111</td><td>1980</td><td>M01</td><td>0.203</td></tr><tr><td>2</td><td>APU0000701111</td><td>1980</td><td>M02</td><td>0.205</td></tr><tr><td>3</td><td>APU0000701111</td><td>1980</td><td>M03</td><td>0.211</td></tr></tbody></table>

To switch databases, just invoke the DB argument again with a different database:

	In [5]: %%sql DB=sports
	        SELECT * 
	        FROM nba LIMIT 3
<table class="table-striped table-hover" id="table9b65a0c1-3313-4e7c-9227-006f5c4d522b" width="100%"><thead><tr><th> </th><th>dateof</th><th>team</th><th>opp</th><th>pts</th><th>fg</th><th>fg_att</th><th>ft</th><th>ft_att</th><th>fg3</th><th>fg3_att</th><th>off_rebounds</th><th>def_rebounds</th><th>asst</th><th>blks</th><th>fouls</th><th>stls</th><th>turnovers</th></tr></thead><tbody><tr><td>1</td><td>2015-10-27</td><td>DET</td><td>ATL</td><td>106</td><td>37</td><td>96</td><td>20</td><td>26</td><td>12</td><td>29</td><td>23</td><td>36</td><td>23</td><td>3</td><td>15</td><td>5</td><td>15</td></tr><tr><td>2</td><td>2015-10-27</td><td>ATL</td><td>DET</td><td>94</td><td>37</td><td>82</td><td>12</td><td>15</td><td>8</td><td>27</td><td>7</td><td>33</td><td>22</td><td>4</td><td>25</td><td>9</td><td>15</td></tr><tr><td>3</td><td>2015-10-27</td><td>CLE</td><td>CHI</td><td>95</td><td>38</td><td>94</td><td>10</td><td>17</td><td>9</td><td>29</td><td>11</td><td>39</td><td>26</td><td>7</td><td>21</td><td>5</td><td>11</td></tr></tbody></table>

####`PATH` Parameter

To write the data to a CSV, use the PATH argument:

	In [6]: %%sql PATH='/<path>/<to>/<file>.csv'
	        SELECT * 
	        FROM nba LIMIT 3
<table class="table-striped table-hover" id="table9b65a0c1-3313-4e7c-9227-006f5c4d522b" width="100%"><thead><tr><th> </th><th>dateof</th><th>team</th><th>opp</th><th>pts</th><th>fg</th><th>fg_att</th><th>ft</th><th>ft_att</th><th>fg3</th><th>fg3_att</th><th>off_rebounds</th><th>def_rebounds</th><th>asst</th><th>blks</th><th>fouls</th><th>stls</th><th>turnovers</th></tr></thead><tbody><tr><td>1</td><td>2015-10-27</td><td>DET</td><td>ATL</td><td>106</td><td>37</td><td>96</td><td>20</td><td>26</td><td>12</td><td>29</td><td>23</td><td>36</td><td>23</td><td>3</td><td>15</td><td>5</td><td>15</td></tr><tr><td>2</td><td>2015-10-27</td><td>ATL</td><td>DET</td><td>94</td><td>37</td><td>82</td><td>12</td><td>15</td><td>8</td><td>27</td><td>7</td><td>33</td><td>22</td><td>4</td><td>25</td><td>9</td><td>15</td></tr><tr><td>3</td><td>2015-10-27</td><td>CLE</td><td>CHI</td><td>95</td><td>38</td><td>94</td><td>10</td><td>17</td><td>9</td><td>29</td><td>11</td><td>39</td><td>26</td><td>7</td><td>21</td><td>5</td><td>11</td></tr></tbody></table>


####`MAKE_GLOBAL` parameter
And my favorite. You can assign the dataframe to a variable like this useing the MAKE_GLOBAL argument:

	In [9]: %%sql MAKE_GLOBAL=WHATEVER_NAME_YOU_WANT DB=bls
	        SELECT * 
	        FROM la_unemployment
	        WHERE year = 1976
	            AND period = 'M01'
	        LIMIT 3
<table class="table-striped table-hover" id="table44533bf5-37d3-4988-a70d-fa05eeef28f9" width="100%"><thead><tr><th> </th><th>series_id</th><th>year</th><th>period</th><th>value</th><th>footnote_codes</th></tr></thead><tbody><tr><td>1</td><td>LASST470000000000003</td><td>1976</td><td>M01</td><td>6.2</td><td>None</td></tr><tr><td>2</td><td>LASST470000000000004</td><td>1976</td><td>M01</td><td>111152.0</td><td>None</td></tr><tr><td>3</td><td>LASST470000000000005</td><td>1976</td><td>M01</td><td>1691780.0</td><td>None</td></tr></tbody></table>

And call the variable:

	In [10]: WHATEVER_NAME_YOU_WANT
<table class="table-striped table-hover" id="table44533bf5-37d3-4988-a70d-fa05eeef28f9" width="100%"><thead><tr><th> </th><th>series_id</th><th>year</th><th>period</th><th>value</th><th>footnote_codes</th></tr></thead><tbody><tr><td>1</td><td>LASST470000000000003</td><td>1976</td><td>M01</td><td>6.2</td><td>None</td></tr><tr><td>2</td><td>LASST470000000000004</td><td>1976</td><td>M01</td><td>111152.0</td><td>None</td></tr><tr><td>3</td><td>LASST470000000000005</td><td>1976</td><td>M01</td><td>1691780.0</td><td>None</td></tr></tbody></table>

####`RAW` Parameter
You can also return the raw RowProxy from SQLAlchemy by setting the RAW argument to `True` and using the `MAKE_GLOBAL`
argument.

	In [10]: %%sql MAKE_GLOBAL=data RAW=True
	         SELECT * 
	         FROM la_unemployment
	         LIMIT 3

	In [11]: data
	         [(u'LASST470000000000003', 1976, u'M01', 6.2, None),
	          (u'LASST470000000000003', 1976, u'M02', 6.1, None),
	          (u'LASST470000000000003', 1976, u'M03', 6.0, None)]

####`DISPLAY` Parameter
Query the data without rendering the table (useful if the result set is prohibitively large and displaying the table breaks things) by setting the `DISPLAY` parameter to `False`. It makes sense to use this parameter in conjunction with the `MAKE_GLOBAL` parameter so the data is passed to the variable but the table isn't rendered:

	In [10]: %%sql MAKE_GLOBAL=data DISPLAY=False
	         SELECT * 
	         FROM la_unemployment

####`ENGINE` Parameter
The `ENGINE` parameter accepts any connection string and creates a connection based on that.

	In [10]: %%sql ENGINE='postgresql://username:password@host:port/DB'
	         SELECT * 
	         FROM la_unemployment
	         LIMIT 3
    
####`TRANSACTION_BLOCK` Parameter
Some SQL statements (`VACUUM`, `CREATE <db>`, `DROP <db>`, etc.) must be executed outside of a transaction block by setting the isolation level to 0 (see <a href="https://www.postgresql.org/docs/9.1/static/transaction-iso.html">this</a>)

	In [10]: %%sql TRANSACTION_BLOCK=False
	         VACUUM ANALYZE <table_name>
		 
####`EDIT` Parameter
Enables inline editing.

	In [10]: %%sql EDIT=True
	         SELECT * 
	         FROM la_unemployment
	         LIMIT 3

Will display a table where the cells can be clicked on and edited.


###Pass Python variables to SQL

To pass python variables to your queries, just do the following.

	In[7]: # define your parameters in a python cell
	        name = '1976'
	        period = 'M01'
	
Now in a `%%sql` cell:

	In [8]: %%sql DB=bls
	        SELECT * 
	        FROM la_unemployment
	        WHERE year = %(year)s
	            AND period = %(period)s
	        LIMIT 3

You can also use a colon to indicate your variables:

	In [8]: %%sql DB=bls
	        SELECT * 
	        FROM la_unemployment
	        WHERE year = :year
	            AND period = :period
	        LIMIT 3
		
Both output the following table:
		
<table class="table-striped table-hover" id="tableea46889f-5850-4a5d-9b78-af10c7387e1d" width="100%"><thead><tr><th> </th><th>series_id</th><th>year</th><th>period</th><th>value</th><th>footnote_codes</th></tr></thead><tbody><tr><td>1</td><td>LASST470000000000003</td><td>1976</td><td>M01</td><td>6.2</td><td>None</td></tr><tr><td>2</td><td>LASST470000000000004</td><td>1976</td><td>M01</td><td>111152.0</td><td>None</td></tr><tr><td>3</td><td>LASST470000000000005</td><td>1976</td><td>M01</td><td>1691780.0</td><td>None</td></tr></tbody></table>

###`psql` metacommands

	In [1]: %%sql DB=bls
	        \dp
<table class="table-striped table-hover" id="table31a4a78e-69dc-40db-9085-6c5788e63e5c" width="100%"><thead><tr><th> </th><th>Schema</th><th>Name</th><th>Type</th><th>Access privileges</th><th>Column privileges</th><th>Policies</th></tr></thead><tbody><tr><td>1</td><td>public</td><td>avg_price</td><td>table</td><td>nan</td><td>nan</td><td>nan</td></tr><tr><td>2</td><td>public</td><td>la_unemployment</td><td>table</td><td>nan</td><td>nan</td><td>nan</td></tr><tr><td>3</td><td>public</td><td>tu_atus</td><td>table</td><td>nan</td><td>nan</td><td>nan</td></tr></tbody></table>

	In [2]: %%sql
	        \d avg_price
<table class="table-striped table-hover" id="table54690703-ce56-42ab-b574-d0f2c13244bc" width="100%"><thead><tr><th> </th><th>Column</th><th>Type</th><th>Modifiers</th></tr></thead><tbody><tr><td>1</td><td>series_id</td><td>character varying(17)</td><td>nan</td></tr><tr><td>2</td><td>year</td><td>integer</td><td>nan</td></tr><tr><td>3</td><td>period</td><td>character varying(3)</td><td>nan</td></tr><tr><td>4</td><td>value</td><td>real</td><td>nan</td></tr></tbody></table>

	In [3]: %%sql DB=sports
	        \COPY public.nba (dateof, team, opp, pts, fouls) to '/<path>/<to>/<file>.csv'
		
	Out[3]: <p>COPY 3092</p>

###Multi-threading

All queries are executed on their own thread, so you can run as many queries as your box will allow while concurrently executing python code.

###Buttons

![lots_of_buttons](images/buttons.png?raw=true)

Buttons include </br> • Viewing Query Plan with d3.js sankey graph </br> • Running Explain Analyze on your query </br> • executing query </br> • executing query and returning SQLAlchemy results in a variable </br> • saving to a TSV </br> • stopping query </br> • swithcing between user-defined engines 

###Inline editing

Set the `EDIT` parameter to `True` to enable inline editing. As long as you are querying one table and that table has a primary key, then you can edit it using the UI.

![inline editing](images/Screen Shot 2016-11-08 at 8.32.56 PM.png)

###Easy to read Query Plan table
This includes a heatmap-like color scale to indicate problem spots in your query.

![query_plan_table](images/query_plan_table.png)

##Pipeline (Added)

###Easy to read Query Plan graph
Currently in dev, this is a sankey graph and also includes a heatmap-like color scale to indicate problem spots in your query, built with D3.js.

![query_plan_graph](images/query_plan_graph.png)


And that's it. 

Enjoy and contribute.

See more <a href="https://gist.github.com/tmthyjames/1366b21d0efffb73f1a91361a25b9a55">here</a>.
