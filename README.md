# SQLCell
Use magic function to run SQL queries in Jupyter Notebook. <a href="https://gist.github.com/tmthyjames/1366b21d0efffb73f1a91361a25b9a55">Blog post here</a>

In less than one hundred lines of Python, you can get rid of your favorite SQL interface and
use a Jupyter notebook to run queries with as little as 

    %%sql DB=YOUR_DATABASE_NAME
    your query
    
Just clone the repo and `cp` the file to Jupyter's startup directory:

    $ git clone https://github.com/tmthyjames/SQLCell.git
    $ cp sqlcell.py ~/.ipython/profile_default/startup/sqlcell.py
    
Then in the Jupyter notebook, define your connection variables. You can also input these directly 
in the sqlcell.py script so you don't have to add them everytime. Just find the line that says
`# default connection string info here` and enter the connection details there.

    driver = 'postgresql'
    username = 'tdobbins'
    password = 'tdobbins'
    host = 'localhost'
    port = '5432'
    
Now you are ready to ditch pgAdmin or whatever SQL interface you use.
    
And that's it.

After you specify the database name the first time, you won't have to do so again for the rest
of the Jupyter session unless you want to change databases.

See more <a href="https://gist.github.com/tmthyjames/1366b21d0efffb73f1a91361a25b9a55">here</a>.
