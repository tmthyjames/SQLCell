from IPython.display import display, Javascript
from ipywidgets import Button, HBox, VBox
from multiprocessing.pool import ThreadPool

class TabCompleter(object):
    pass
    
class BackgroundHandler(object):
    """bg=True"""
    pass

class ControlPanel(object):
    # def run_btn_callback(evt): # figure out how to execute a cell
    #     Javascript("""
    #         var CodeCell = __webpack_require__(/*! @jupyterlab/cells */ "USP6").CodeCell
    #         CodeCell.execute()
    #     """)


    # def get_control_panel(): # remove until I can get JS working
    #     run_btn = Button(description='run')
    #     run_btn.on_click(run_btn_callback)
    #     stop_btn = Button(description='stop')
    #     bg_btn = Button(description='bg')

    #     left_box = VBox([run_btn])
    #     center_box = VBox([stop_btn])
    #     right_box = VBox([bg_btn])
    #     control_panel = HBox([left_box, center_box, right_box])
    #     return control_panel
    pass
