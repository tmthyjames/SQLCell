
def buttons_js(unique_id, __ENGINES_JSON_DUMPS__, unique_db_id, db):
    js = '''
        <style>
        .input { 
            position:relative; 
        }
        #childDiv'''+unique_id+''' { 
            width: 90%;
            position:absolute; 
        }
        #table'''+unique_id+'''{
            padding-top: 40px;
        }
        </style>
        <div class="row" id="childDiv'''+unique_id+'''">
            <div class="btn-group col-md-3">
                <button id="explain" title="Explain Analyze Graph" onclick="explain('__EXPLAIN_GRAPH__')" type="button" class="btn btn-info btn-sm"><p class="fa fa-code-fork fa-rotate-270"</p></button>
                <button id="explain" title="Explain Analyze" onclick="explain('__EXPLAIN__')" type="button" class="btn btn-info btn-sm"><p class="fa fa-info-circle"</p></button>
                <button type="button" title="Execute" onclick="run()" class="btn btn-success btn-sm"><p class="fa fa-play"></p></button>
                <button type="button" title="Execute and Return Data as Variable" onclick="getData()" class="btn btn-success btn-sm"><p class="">var</p></button>
                <button id="saveData'''+unique_id+'''" title="Save" class="btn btn-success btn-sm disabled" type="button"><p class="fa fa-save"</p></button>
                <button id="cancelQuery'''+unique_id+'''" title="Cancel Query" class="btn btn-danger btn-sm" type="button"><p class="fa fa-stop"</p></button>
            </div>
            <div id="engineButtons'''+unique_id+'''" class="btn-group col-md-4"></div>
            <div id="tableData'''+unique_id+'''"></div>
        </div>
        <div class="table" id="table'''+unique_id+'''"></div>
        <script type="text/Javascript">

            (function($) {
                var MutationObserver = window.MutationObserver || window.WebKitMutationObserver || window.MozMutationObserver;

                $.fn.attrchange = function(callback) {
                    if (MutationObserver) {
                        var options = {
                            subtree: false,
                            attributes: true
                        };

                        var observer = new MutationObserver(function(mutations) {
                            mutations.forEach(function(e) {
                                callback.call(e.target, e.attributeName);
                            });
                        });

                        return this.each(function() {
                            observer.observe(this, options);
                        });

                    }
                }
            } )(jQuery);
            $("#childDiv'''+unique_id+'''").parents('.code_cell').attrchange(function(attrName){
                if (attrName=='class'){
                    if ($(this).hasClass('unselected')){
                        $("#childDiv'''+unique_id+'''").find('button').each(function(i, obj){
                            $(obj).addClass('disabled');
                        });
                    } else if ($(this).hasClass('selected')){
                        $("#childDiv'''+unique_id+'''").find('button').each(function(i, obj){
                            $(obj).removeClass('disabled');
                        });
                    }
                }
            });


        
            var engines = JSON.parse(`'''+str(__ENGINES_JSON_DUMPS__)+'''`);
            
            var sortedEngineKeys = Object.keys(engines).sort(function(a,b){
                return engines[a].order - engines[b].order;
            });
            
            var sortedEngines = sortedEngineKeys.reduce(function(output, row, idx){
                engines[row]['key'] = row;
                output.push(engines[row]);
                return output;
            }, []);
            
            var engineButtons = '';
            for (var engine in sortedEngines){
                var engineKey = sortedEngines[engine].key;
                var warningLabel = sortedEngines[engine].caution_level;
                engineButtons += '<button title="Switch Engine" onclick="switchEngines('+"'"+engineKey+"'"+')" class="btn btn-'+warningLabel+' btn-sm">'+engineKey+'</button>';
            };
            
           $("#engineButtons'''+unique_id+'''").append(engineButtons);
           
           $("#cancelQuery'''+unique_id+'''").on('click', function(){
               if ($("#cancelQuery'''+unique_id+'''").hasClass('disabled')){
                   console.log('alread stopped or finished query...');
               } else {
                   cancelQuery("'''+unique_db_id+'''");
                   //$("#cancelQuery'''+unique_id+'''").addClass('disabled')
               }
           });
        
            function explain(gloVar){
                var command =  `__SQLCell_GLOBAL_VARS__.`+gloVar+` = True`;
                console.log(command);
                var kernel = IPython.notebook.kernel;
                kernel.execute(command);
                IPython.notebook.execute_cell();
            };
            
            function run(){
                IPython.notebook.execute_cell();
                $.get('/api/contents', function(data){
                    console.log(data);
                });
            };
            
            function cancelQuery(applicationID){
                IPython.notebook.kernel.execute('__SQLCell_GLOBAL_VARS__().kill_last_pid_on_new_thread(__SQLCell_GLOBAL_VARS__.jupyter_id, "'''+db+'''", "'''+unique_id+'''")',
                    {
                        iopub: {
                            output: function(response) {
                                var $table = $("#table'''+unique_id+'''").parent();
                                console.log(response);
                                if (response.content && response.content.text){
                                    $table.append('<h5 style="color:#d9534f;">'+response.content.text+'</h5>')
                                } else if (response.content && response.content.evalue){
                                    $table.append('<h5 style="color:#d9534f;">'+response.content.evalue+'</h5>');
                                }
                            }
                        }
                    },
                    {
                        silent: false, 
                        store_history: false, 
                        stop_on_error: true
                    }
                );
            };
            
            function getData(){
                var command = `__SQLCell_GLOBAL_VARS__.__GETDATA__ = True`;
                var kernel = IPython.notebook.kernel;
                kernel.execute(command);
                IPython.notebook.execute_cell();
            };
            
            function saveData(data, filename){
                var path = $('#path').val() || './'
                var command = `__SQLCell_GLOBAL_VARS__.__SAVEDATA__, PATH = True,'`+path+`'`;
                var kernel = IPython.notebook.kernel;
                kernel.execute(command);
                //IPython.notebook.execute_cell();
                
                function download(data, filename, type) {
                    var a = document.createElement("a"),
                        file = new Blob([data], {type: type});
                    if (window.navigator.msSaveOrOpenBlob) // IE10+
                        window.navigator.msSaveOrOpenBlob(file, filename);
                    else { // Others
                        var url = URL.createObjectURL(file);
                        a.href = url;
                        a.download = filename;
                        document.body.appendChild(a);
                        a.click();
                        setTimeout(function() {
                            document.body.removeChild(a);
                            window.URL.revokeObjectURL(url);  
                        }, 0); 
                    }
                }
                
                download(data, filename, 'csv');
                
            };
            
            function switchEngines(engine){
                var command = "__SQLCell_GLOBAL_VARS__.ENGINE = " + "'" + engines[engine].engine + "'''+db+'''" + "'";
                var kernel = IPython.notebook.kernel;
                kernel.execute(command,{
                    iopub: {
                        output: function(response) {
                            var $table = $("#table'''+unique_id+'''").parent();
                            console.log(response);
                            if (response.content && response.content.text){
                                $table.append('<h5 style="color:#d9534f;">'+response.content.text+'</h5>')
                            } else if (response.content && response.content.evalue){
                                $table.append('<h5 style="color:#d9534f;">'+response.content.evalue+'</h5>');
                            }
                        }
                    }
                },
                    {
                        silent: false, 
                        store_history: false, 
                        stop_on_error: true
                    }
                );
                IPython.notebook.execute_cell();
            };

        </script>
        '''
    return js

def notify_js(unique_id, cell, t1, df, engine):
    notification = """
        if ($.notify){
            $.notify({},{
                delay: 5000,
                animate: {
                    enter: 'animated fadeInRight',
                    exit: 'animated fadeOutRight'
                },
                allow_dismiss: true,
                mouse_over: "pause",
                template: '<div data-notify="container" class="col-xs-11 col-sm-3 alert alert-info" role="alert">' +
                    '<button type="button" aria-hidden="true" class="close" data-notify="dismiss">x</button>' +
                    '<div style="cursor:pointer;" data-notify="container" onclick="document.getElementById(`table%s`).scrollIntoView();">' +
                        '<span data-notify="title"><strong>Query Finished</strong></span>' +
                        `</br><span data-notify="message"><pre style=\"max-height:150px;overflow-y:scroll;\">%s</pre>To Execute: %s | Rows: %s | DB: %s | Host: %s</span>` +
                    '</div>' +
                '</div>'
            });
        } else {
            console.log('$.notify is not a function. trouble loading bootstrap-notify.')
        }
    """ % (unique_id, cell.replace("\\", "\\\\"), str(round(t1, 3)), len(df.index), engine.url.database, engine.url.host)
    return notification

def sankey_js(unique_id, query_plan_depth, query_plan):
    sankey_graph = """
        <style>
        .node rect {
          cursor: move;
          fill-opacity: .9;
          shape-rendering: crispEdges;
        }

        .node text {
          pointer-events: none;
          text-shadow: 0 0px 0 #fff;
        }

        .link {
          fill: none;
          stroke: #000;
          stroke-opacity: .2;
        }

        .link:hover {
          stroke-opacity: .5;
        }
        div.output_area img, div.output_area svg{ 
            max-width:none;
        }
        </style>
        <div id='table"""+unique_id+"""'></div>
        <script>
        var margin = {top: 10,right: 1,bottom: 6,left: 1},
            width = Math.max("""+str(query_plan_depth*125)+""", 1000) - margin.left - margin.right,
            height = 500 - margin.bottom;

        var formatNumber = d3.format(",.0f"),
            format = function(d) {
                return formatNumber(d);
            },
            color = d3.scale.category20();

        var svg = d3.select('#table"""+unique_id+"""').append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        var sankey = d3.sankey()
            .nodeWidth(15)
            .nodePadding(50)
            .size([width, height]);

        var path = sankey.link();
        var energy = """+query_plan+"""
        var executionTime = energy.executionTime
        energy = {
            nodes: energy.nodes,
            links: energy.links
        };
        sankey
            .nodes(energy.nodes)
            .links(energy.links)
            .layout(32);
        var link = svg.append("g").selectAll(".link")
            .data(energy.links)
            .enter().append("path")
            .attr("class", "link")
            .attr("d", path)
            .style("stroke-width", function(d) {
                return Math.max(1, d.dy);
            })
            .sort(function(a, b) {
                return b.dy - a.dy;
            });

        link.append("title")
            .html(function(d) {
                return d.source.nodetype + " -> " 
                    + d.target.nodetype + "<br/>" 
                    + 'Total Cost: ' + format(d.value) + "<br/>"
                    + 'Child Rows: ' + format(d.source.rows) + "<br/>"
                    + 'Parent Rows: ' + format(d.target.rows);
            });

        var node = svg.append("g").selectAll(".node")
            .data(energy.nodes)
            .enter().append("g")
            .attr("class", "node")
            .attr("transform", function(d) {
                return "translate(" + d.x + "," + d.y + ")";
            })
            .call(d3.behavior.drag()
                .origin(function(d) {
                    return d;
                })
                .on("dragstart", function() {
                    this.parentNode.appendChild(this);
                })
                .on("drag", dragmove));

        node.append("rect")
            .attr("height", function(d) {
                return Math.max(d.dy, 3);
            })
            .attr("width", sankey.nodeWidth())
            .style("fill", function(d) {
                if ((d.endtime - d.starttime) > (executionTime * 0.9)) return d.color = "#800026"
                else if ((d.endtime - d.starttime) > (executionTime * 0.8)) return d.color = "#bd0026"
                else if ((d.endtime - d.starttime) > (executionTime * 0.7)) return d.color = "#e31a1c"
                else if ((d.endtime - d.starttime) > (executionTime * 0.6)) return d.color = "#fc4e2a"
                else if ((d.endtime - d.starttime) > (executionTime * 0.5)) return d.color = "#fd8d3c"
                else if ((d.endtime - d.starttime) > (executionTime * 0.4)) return d.color = "#feb24c"
                else if ((d.endtime - d.starttime) > (executionTime * 0.3)) return d.color = "#fed976"
                else if ((d.endtime - d.starttime) > (executionTime * 0.2)) return d.color = "#ffeda0"
                else if ((d.endtime - d.starttime) > (executionTime * 0.1)) return d.color = "#ffffcc"
                else return d.color = "#969696"
            })
            .append("title")
            .html(function(d) { 
                return (d.display || '') + "<br/>Cost: " 
                    + formatNumber(d.value) + "<br/>Time: " 
                    + d.starttime + '...' + d.endtime
                    + '<br/>Rows: ' + formatNumber(d.rows);
            });

        node.append("text")
            .attr("x", -6)
            .attr("y", function(d) {
                return d.dy / 2;
            })
            .attr("dy", ".35em")
            .attr("text-anchor", "end")
            .attr("transform", null)
            .text(function(d) {
                return d.subplan || d.nodetype;
            })
            .filter(function(d) {
                return d.x < width / 2;
            })
            .attr("x", 6 + sankey.nodeWidth())
            .attr("text-anchor", "start");

        function dragmove(d) {
            d3.select(this).attr("transform", "translate(" + d.x + "," + (d.y = Math.max(0, Math.min(height - d.dy, d3.event.y))) + ")");
            sankey.relayout();
            link.attr("d", path);
        }
        </script>
        """
    return sankey_graph

def table_js(unique_id, table_name, primary_key):
    table = """
        $('#table%s').editableTableWidget({preventColumns:[1,2]});
        $('#table%s').on('change', function(evt, newValue){
            var oldValue = evt.target.attributes[0].value, oldValueMatch;
            console.log(oldValue);
            var th = $('#table%s th').eq(evt.target.cellIndex);
            var columnName = th.text();

            var tableName = '%s';
            var primary_key = '%s';

            var pkId,
                pkValue;
            $('#table%s tr th').filter(function(i,v){
                if (v.innerHTML == primary_key){
                    pkId = i;
                }
            });

            var row = $('#table%s > tbody > tr').eq(evt.target.parentNode.rowIndex-1);
            row.find('td').each(function(i,v){
                if (i == pkId){
                    pkValue = v.innerHTML;
                }
            });

            oldValueMatch = oldValue.match(/^[0-9]/) ? oldValue : "'" + oldValue + "'";
            pkValue = columnName == primary_key ? oldValueMatch : pkValue;

            var SQLText = "UPDATE " + tableName + " SET " + columnName + " = '" + newValue + "' WHERE " + primary_key + " = " + pkValue;
            console.log(SQLText);

            if (pkValue === ''){
            } else {
                $('#error').remove();
                console.log(newValue, oldValue)
                IPython.notebook.kernel.execute('__SQLCell_GLOBAL_VARS__.update_table("'+SQLText+'")',
                    {
                        iopub: {
                            output: function(response) {
                                var $table = $('#table%s').parent();
                                if (response.content.evalue){
                                    var error = response.content.evalue.replace(/\\n/g, "</br>");
                                    $table.append('<h5 id="error" style="color:#d9534f;">'+error+'</h5>');
                                    evt.target.innerHTML = oldValue;
                                } else {
                                    $table.append('<h5 id="error" style="color:#5cb85c;">Update successful</h5>');
                                    evt.target.attributes[0].value = newValue;
                                }
                            }
                        }
                    },
                    {
                        silent: false, 
                        store_history: false, 
                        stop_on_error: true
                    }
                );
            }
        });
        """ % (unique_id, unique_id, unique_id, table_name, primary_key, unique_id, unique_id, unique_id)
    return table

def psql_table_js(unique_id, table_name):
    table = """
        $('#table%s').editableTableWidget({preventColumns:[1]});
        $('#table%s').on('change', function(evt, newValue){
            var tableName = '%s';
            var oldValue = evt.target.attributes[0].value;
            var th = $('#table%s th').eq(evt.target.cellIndex);
            var columnName = th.text();
            var SQLText;
            if (columnName == 'Column'){
                SQLText = "ALTER TABLE " + tableName + " RENAME COLUMN " + oldValue + " TO " + newValue;
            } else if (columnName == 'Type') {
                var columnName = evt.target.previousSibling.innerHTML;
                SQLText = "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " TYPE " + newValue;
            }

            IPython.notebook.kernel.execute('__SQLCell_GLOBAL_VARS__.update_table("'+SQLText+'")',
                {
                    iopub: {
                        output: function(response) {
                            var $table = $('#table%s').parent();
                            if (response.content.evalue){
                                var error = response.content.evalue.replace(/\\n/g, "</br>");
                                $table.append('<h5 id="error" style="color:#d9534f;">'+error+'</h5>');
                                evt.target.innerHTML = oldValue;
                            } else {
                                $table.append('<h5 id="error" style="color:#5cb85c;">Update successful</h5>');
                                evt.target.attributes[0].value = newValue;
                            }
                        }
                    }
                },
                {
                    silent: false, 
                    store_history: false, 
                    stop_on_error: true
                }
            );
        });
        """ % (unique_id, unique_id, table_name, unique_id, unique_id)
    return table

def load_js_scripts():
    scripts = """
            $.getScript('//rawgit.com/tmthyjames/SQLCell/feature/%2361-sqlcell/js/bootstrap-notify.min.js', function(resp, status){
                $('head').append(
                    '<link rel="stylesheet" href="//cdn.rawgit.com/tmthyjames/SQLCell/feature/%2361-sqlcell/css/animate.css" type="text/css" />' 
                );
                console.log('animate.css loaded');

            });
            $.getScript('//d3js.org/d3.v3.min.js', function(resp, status){
                console.log(resp, status, 'd3');
                $.getScript('//cdn.rawgit.com/tmthyjames/SQLCell/bootstrap-notify/js/sankey.js', function(i_resp, i_status){
                    console.log(i_resp, i_status, 'd3.sankey');
                });
            });

            $.getScript('//cdn.rawgit.com/tmthyjames/SQLCell/bootstrap-notify/js/editableTableWidget.js', function(resp, status){
                console.log(resp, status, 'editableTableWidget')
            });
        """
    return scripts