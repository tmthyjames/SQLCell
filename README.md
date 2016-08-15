# SQLCell
Uses Jupyter magic function to run SQL queries in Jupyter Notebook. <a href="https://gist.github.com/tmthyjames/1366b21d0efffb73f1a91361a25b9a55">Blog post here</a>.

In less than one hundred lines of Python, you can get rid of your favorite SQL interface and
use a Jupyter notebook to run queries with as little as 

    %%sql
    your query
    
Just clone the repo and `cp` the file to Jupyter's startup directory:

    $ git clone https://github.com/tmthyjames/SQLCell.git
    $ cp sqlcell.py ~/.ipython/profile_default/startup/sqlcell.py
    
Then in the Jupyter notebook, define your connection variables. You can also input these directly 
in the sqlcell.py script so you don't have to add them everytime. Just find the line that says
`# default connection string info here` and enter the connection details there.

    driver = 'postgresql'
    username = 'username'
    password = 'password'
    host = 'host'
    port = '5432'
    
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

![make_global](images/named_params.png?raw=true)

And my favorite. You can assign the dataframe to a variable like this useing the MAKE_GLOBAL argument:

![make_global](images/make_global.png?raw=true)

With all arguments specified:

![make_global](images/all_options.png?raw=true)

And for those few people who have Jupyter but not pandas, I thought about you. If you have pandas, a dataframe
will be returned. If not, you'll get a simple HTML table to view the data, without any additional configuration.

![make_global](images/without_pandas.png?raw=true)
    
And that's it. 

Enjoy and contribute.

See more <a href="https://gist.github.com/tmthyjames/1366b21d0efffb73f1a91361a25b9a55">here</a>.
