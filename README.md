*** UPDATE ***  -  11/017/2016

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

    %%sql
    your query
    
Just clone the repo and `cp` the app to Jupyter's startup directory:

    $ cd .ipython/profile_default/startup # or wherever your startup directory is
    $ git clone https://github.com/tmthyjames/SQLCell.git
    $ cp SQLCell/sqlcell_app.py sqlcell_app.py # place app.py in the startup folder so it will get executed
    
Then in the engine_config.py file, define your connection variables. If you don't add them to engine_config.py, 
then you'll have to pass a connection string to the ENGINE parameter everytime you use `%%sql`, like so:

	%%sql ENGINE='postgresql://username:password@host:port/database'
	SELECT * FROM table;
    
Now you are ready to ditch pgAdmin or whatever SQL interface you use. Continue reading to see all the available 
options, like writing results to a CSV, using SQLAlchemy named parameters and more.

After adding your connection details to sqlcell.py, run your first query with the DB argument:

![make_global](images/initial.png?raw=true)

For the rest of the session, you won't have to use the DB argument unless you want to change
databases.

![make_global](images/second_run.png?raw=true)

To switch databases, just invoke the DB argument again with a different database:

![make_global](images/change_db_name.png?raw=true)

To write the data to a CSV, use the PATH argument:

![make_global](images/to_csv.png?raw=true)

To use SQLAlchemy's named parameters (it's nice to just copy and paste if you use Jupyter for 
development/editing and don't want to delete all SQLALchemy paramters just to run a query), use
the PARAMS argument.

	In[1]: # define your parameters in a python cell
	name = '1976'
	period = 'M01'
	
Now in a `%%sql` cell:

	%%sql
	SELECT * 
	FROM la_unemployment
	WHERE year = %(year)s
		AND period = %(period)s
	LIMIT 5

And my favorite. You can assign the dataframe to a variable like this useing the MAKE_GLOBAL argument:

![make_global](images/make_global.png?raw=true)

With all arguments specified:

![make_global](images/all_options.png?raw=true)

And for those few people who have Jupyter but not pandas, I thought about you. If you have pandas, a dataframe
will be returned. If not, you'll get a simple HTML table to view the data, without any additional configuration.

![make_global](images/without_pandas.png?raw=true)

NEW FEATURES as of Aug 16, 2016:

You can now use python variables without the `PARAMS` argument

![make_global](images/remove_PARAMS.png?raw=true)

You can also return the raw RowProxy from SQLAlchemy by setting the RAW argument to `True` and using the `MAKE_GLOBAL`
argument.

![make_global](images/returnraw.png?raw=true)
    
And that's it. 

Enjoy and contribute.

See more <a href="https://gist.github.com/tmthyjames/1366b21d0efffb73f1a91361a25b9a55">here</a>.
