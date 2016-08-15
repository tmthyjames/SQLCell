# SQLCell
Use magic function to run SQL queries in Jupyter Notebook. <a href="https://gist.github.com/tmthyjames/1366b21d0efffb73f1a91361a25b9a55">Blog post here</a>

In less than one hundred lines of Python, you can get rid of your favorite SQL interface and
use a Jupyter notebook to run queries with as little as 

    %%sql DB=YOUR_DATABASE_NAME
    your query
    
Just clone the repo and cp the file to Jupyter's startup directory:

    $ git clone https://github.com/tmthyjames/SQLCell.git
    $ cp sqlcell.py ~/.ipython/profile_default/startup/sqlcell.py
    
And that's it.

After you specify the database name the first time, you won't have to do so again for the rest
of the Jupyter session unless you want to change databases.
