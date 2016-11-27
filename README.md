*** UPDATE ***  -  11/22/2016

SQLCell now accepts psql meta commands

	In [1]: %%sql DB=bls
	        \dp
<table class="table-striped table-hover" id="table31a4a78e-69dc-40db-9085-6c5788e63e5c" width="100%"><thead><tr><th> </th><th>Schema</th><th>Name</th><th>Type</th><th>Access privileges</th><th>Column privileges</th><th>Policies</th></tr></thead><tbody><tr><td>1</td><td>public</td><td>avg_price</td><td>table</td><td>nan</td><td>nan</td><td>nan</td></tr><tr><td>2</td><td>public</td><td>la_unemployment</td><td>table</td><td>nan</td><td>nan</td><td>nan</td></tr><tr><td>3</td><td>public</td><td>tu_atus</td><td>table</td><td>nan</td><td>nan</td><td>nan</td></tr></tbody></table>

	In [2]: %%sql DB=bls
	        \d avg_price
<table class="table-striped table-hover" id="table54690703-ce56-42ab-b574-d0f2c13244bc" width="100%"><thead><tr><th> </th><th>Column</th><th>Type</th><th>Modifiers</th></tr></thead><tbody><tr><td>1</td><td>series_id</td><td>character varying(17)</td><td>nan</td></tr><tr><td>2</td><td>year</td><td>integer</td><td>nan</td></tr><tr><td>3</td><td>period</td><td>character varying(3)</td><td>nan</td></tr><tr><td>4</td><td>value</td><td>real</td><td>nan</td></tr></tbody></table>

	In [3]: %%sql DB=sports
	        \COPY public.nba (dateof, team, opp, pts, fouls) to '/<path>/<to>/<file>.csv'
		
	Out[3]: <p>COPY 3092</p>

*** UPDATE ***  -  11/17/2016

All SQL queries are now executed on separate threads so you can run multiple queries and Python code concurrently.

*** UPDATE ***  -  11/08/2016

Introducing inline editing

![inline editing](images/Screen Shot 2016-11-08 at 8.32.56 PM.png)

*** UPDATE ***

Introducing buttons!

![lots_of_buttons](images/buttons.png?raw=true)

Buttons include </br> • Running Explain Analyze on your query </br> • executing query </br> • executing query and returning SQLAlchemy results in a variable </br> • saving to a TSV </br> • stopping query </br> • swithcing between user-defined engines 

# SQLCell
Uses Jupyter magic function to run SQL queries in Jupyter Notebook. <a href="https://gist.github.com/tmthyjames/1366b21d0efffb73f1a91361a25b9a55">Blog post here</a>.

In less than one hundred lines of Python, you can get rid of your favorite SQL interface and
use a Jupyter notebook to run queries with as little as 

    In [1]: %%sql
            your query
    
Just clone the repo and `cp` the `sqlcell_app.py` file to Jupyter's startup directory (on my computer, the directory is `~/.ipython/profile_default/startup`, but may be different depending on your OS and version of IPython/Jupyter):

    $ cd .ipython/profile_default/startup # or wherever your startup directory is
    $ git clone https://github.com/tmthyjames/SQLCell.git
    $ cp SQLCell/sqlcell_app.py sqlcell_app.py # place app.py in the startup folder so it will get executed
    
Then in the engine_config.py file, define your connection variables. If you don't add them to engine_config.py, 
then you'll have to pass a connection string to the ENGINE parameter everytime you use `%%sql`, like so:

	In [2]: %%sql ENGINE='postgresql://username:password@host:port/database'
	        SELECT * FROM table;
    
Now you are ready to ditch pgAdmin or whatever SQL interface you use. Continue reading to see all the available 
options, like writing results to a CSV, using SQLAlchemy named parameters and more.

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

To write the data to a CSV, use the PATH argument:

	In [6]: %%sql DB=sports PATH='/<path>/<to>/<file>.csv'
	        SELECT * 
	        FROM nba LIMIT 3
<table class="table-striped table-hover" id="table9b65a0c1-3313-4e7c-9227-006f5c4d522b" width="100%"><thead><tr><th> </th><th>dateof</th><th>team</th><th>opp</th><th>pts</th><th>fg</th><th>fg_att</th><th>ft</th><th>ft_att</th><th>fg3</th><th>fg3_att</th><th>off_rebounds</th><th>def_rebounds</th><th>asst</th><th>blks</th><th>fouls</th><th>stls</th><th>turnovers</th></tr></thead><tbody><tr><td>1</td><td>2015-10-27</td><td>DET</td><td>ATL</td><td>106</td><td>37</td><td>96</td><td>20</td><td>26</td><td>12</td><td>29</td><td>23</td><td>36</td><td>23</td><td>3</td><td>15</td><td>5</td><td>15</td></tr><tr><td>2</td><td>2015-10-27</td><td>ATL</td><td>DET</td><td>94</td><td>37</td><td>82</td><td>12</td><td>15</td><td>8</td><td>27</td><td>7</td><td>33</td><td>22</td><td>4</td><td>25</td><td>9</td><td>15</td></tr><tr><td>3</td><td>2015-10-27</td><td>CLE</td><td>CHI</td><td>95</td><td>38</td><td>94</td><td>10</td><td>17</td><td>9</td><td>29</td><td>11</td><td>39</td><td>26</td><td>7</td><td>21</td><td>5</td><td>11</td></tr></tbody></table>

To use SQLAlchemy's named parameters (it's nice to just copy and paste if you use Jupyter for 
development/editing and don't want to delete all SQLALchemy paramters just to run a query), use
the PARAMS argument.

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
<table class="table-striped table-hover" id="tableea46889f-5850-4a5d-9b78-af10c7387e1d" width="100%"><thead><tr><th> </th><th>series_id</th><th>year</th><th>period</th><th>value</th><th>footnote_codes</th></tr></thead><tbody><tr><td>1</td><td>LASST470000000000003</td><td>1976</td><td>M01</td><td>6.2</td><td>None</td></tr><tr><td>2</td><td>LASST470000000000004</td><td>1976</td><td>M01</td><td>111152.0</td><td>None</td></tr><tr><td>3</td><td>LASST470000000000005</td><td>1976</td><td>M01</td><td>1691780.0</td><td>None</td></tr></tbody></table>

And my favorite. You can assign the dataframe to a variable like this useing the MAKE_GLOBAL argument:

	In [9]: %%sql MAKE_GLOBAL=WHATEVER_NAME_YOU_WANT
	        SELECT * 
	        FROM la_unemployment
	        WHERE year = %(year)s
	            AND period = %(period)s
	        LIMIT 3
<table class="table-striped table-hover" id="table44533bf5-37d3-4988-a70d-fa05eeef28f9" width="100%"><thead><tr><th> </th><th>series_id</th><th>year</th><th>period</th><th>value</th><th>footnote_codes</th></tr></thead><tbody><tr><td>1</td><td>LASST470000000000003</td><td>1976</td><td>M01</td><td>6.2</td><td>None</td></tr><tr><td>2</td><td>LASST470000000000004</td><td>1976</td><td>M01</td><td>111152.0</td><td>None</td></tr><tr><td>3</td><td>LASST470000000000005</td><td>1976</td><td>M01</td><td>1691780.0</td><td>None</td></tr></tbody></table>

And call the variable:

	In [10]: WHATEVER_NAME_YOU_WANT
<table class="table-striped table-hover" id="table44533bf5-37d3-4988-a70d-fa05eeef28f9" width="100%"><thead><tr><th> </th><th>series_id</th><th>year</th><th>period</th><th>value</th><th>footnote_codes</th></tr></thead><tbody><tr><td>1</td><td>LASST470000000000003</td><td>1976</td><td>M01</td><td>6.2</td><td>None</td></tr><tr><td>2</td><td>LASST470000000000004</td><td>1976</td><td>M01</td><td>111152.0</td><td>None</td></tr><tr><td>3</td><td>LASST470000000000005</td><td>1976</td><td>M01</td><td>1691780.0</td><td>None</td></tr></tbody></table>

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
    
And that's it. 

Enjoy and contribute.

See more <a href="https://gist.github.com/tmthyjames/1366b21d0efffb73f1a91361a25b9a55">here</a>.
