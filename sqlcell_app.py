# should be moved to Jupyter's or IPython's profile_default/startup directory
from IPython.core.magic import register_line_cell_magic
from SQLCell.sqlcell import sql, __KERNEL_VARS__, __SQLCell_GLOBAL_VARS__

sql = register_line_cell_magic(sql)

__KERNEL_VARS__.g = globals()
