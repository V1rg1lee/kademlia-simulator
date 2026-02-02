from dash import dcc, Input, Output, callback, html, dash_table
from plotly.subplots import make_subplots
from rich.console import Console
from pprint import pprint
from decimal import Decimal

import os
import sys
import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go

console = Console()

args = sys.argv[1:]

if len(args) > 0:
    logsdir = args[0]
else:
    logsdir = "log_folder"
    console.print(f"No <logs_dir> argument given. Using 'log_folder'.", style="bold white")

if not os.path.exists(logsdir):
    console.print(f"{logsdir} is NOT a valid path.", style="bold red")
    sys.exit()

csv_files = [_ for _ in os.listdir(logsdir) if '.csv' in _]

if len(csv_files) == 0:
    console.print(f"No files found in {logsdir}.", style="bold red")
    sys.exit()

op_file = [_ for _ in csv_files if 'op' in _][0]
msg_file = [_ for _ in csv_files if 'message' in _ or 'msg' in _][0]

op_df = pd.read_csv(os.path.join(logsdir, op_file), index_col=False)
msg_df = pd.read_csv(os.path.join(logsdir, msg_file), index_col=False)

console.print(f"[INFO] Loaded {len(msg_df)} message records from {msg_file}", style="bold cyan")
console.print(f"[INFO] Loaded {len(op_df)} operation records from {op_file}", style="bold cyan")

# Check if we have Kademlia operations or just GossipSub messages
has_operations = len(op_df) > 0 and 'src' in op_df.columns and len(op_df['src'].dropna()) > 0
console.print(f"[INFO] Mode: {'Kademlia Operations' if has_operations else 'GossipSub Messages'}", style="bold cyan")

if has_operations:
    # Kademlia operations mode
    old_ids = op_df['src']
    mapping = {}
    for i in range(len(old_ids)):
        mapping[old_ids[i]] = i + 1

    new_ids = []
    for id in old_ids:
        new_id = mapping[id]
        new_ids.append(new_id)

    op_df["new_src"] = new_ids
    columns = [{"name": i, "id": i} for i in ['id', 'new_src', 'messages', 'type', 'src', 'start', 'stop']]
    display_df = op_df
else:
    # GossipSub messages mode - show message statistics
    display_df = msg_df.copy()
    display_df['index'] = range(len(display_df))
    columns = [{"name": i, "id": i} for i in display_df.columns]

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div(
    [
        dbc.Row(
            [
                html.H1(
                    "Kademlia Visualiser",
                    style={
                        "width": "25%"
                    }
                ),
                # html.Div(
                #     dcc.Input(
                #         id="path-input",
                #         type="text",
                #         placeholder="Enter the path to the logs here",
                #         # TODO: Delete the next line.
                #         value="./log_folder",
                #         style={
                #             "width": "100%",
                #             "padding": "0.5vh 0.5vw"
                #         }
                #     ),
                #     style={
                #         "width": "20%"
                #     }
                # )
            ],
            style={
                # "outline": "1px solid black",
                "display": "flex",
                "justifyContent": "space-between",
                "alignItems": "center",
                "padding": "0vh 1vw",
                "maxWidth": "100vw",
                "borderBottom": "2px solid #aaa",
                "overflowX": "hidden"
            }
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.P(
                            "Click on an operation to plot it.",
                            style={"textAlign": "center"}
                        ),
                        html.Div(
                            dash_table.DataTable(
                                data=display_df.to_dict("records"), 
                                columns=columns,
                                id="table",
                                style_cell={"textAlign": "center"}
                            ),
                            style={
                                "maxHeight": "90vh",
                                "overflowY": "scroll"
                            }
                        )
                    ],
                    width=4
                ),
                dbc.Col(
                    [
                        dcc.Graph(
                            id="graph", 
                            style={
                                "height": "95vh"
                            }
                        ),
                    ],
                    width=8
                )
            ],
            style={
                "padding": "1vh 1vw",
                "maxWidth": "100vw"
            }
        )
    ],
    style={
        "maxWidth": "100vw",
        "maxHeight": "100vh",
        "overflow": "hidden"
    }
)

@callback(Output("graph", "figure"), Input("table", "active_cell"))
def update_graphs(active_cell):
    # Always generate the message statistics chart for GossipSub mode
    if not has_operations:
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=msg_df.index,
            y=msg_df['msgsOut'] if 'msgsOut' in msg_df.columns else [0]*len(msg_df),
            name='Messages Out',
            marker=dict(color='blue')
        ))
        
        if 'msgsIn' in msg_df.columns:
            fig.add_trace(go.Bar(
                x=msg_df.index,
                y=msg_df['msgsIn'].fillna(0),
                name='Messages In',
                marker=dict(color='green')
            ))
        
        fig.update_layout(
            title="GossipSub Message Statistics",
            xaxis_title="Node Index",
            yaxis_title="Message Count",
            barmode='group',
            height=600,
            showlegend=True
        )
        return fig
    
    # Kademlia operations mode
    if active_cell:
        op_id = active_cell["row_id"]
        
        for _, row in op_df.iterrows():
            if int(row["id"]) == int(op_id):
                op = row
            
        message_ids = [int(x) for x in op["messages"].split("|") if x]
        msg_dsts = list((msg_df.loc[ msg_df["id"].isin(message_ids) ])["dst"])
        
        # ? op_id is the id of the op so that we can plot multiple operations by using the op_id on the y-axis
        # ? op_src is the src node (node all the way on the left)
        # ? msg_dsts is the list of nodes that have received from the op_src (nodes on the line in an ordered fashion)
        x = [op["src"]] + msg_dsts
        y = [op_id] * len(msg_dsts) + [op_id]

        fig = go.Figure(
            go.Scatter(
                x = x, 
                y = y, 
                marker=dict(size = 50)
            )
        )
        
        fig.update_xaxes(type="log")
        fig.update_layout(title="Operation " + str(op_id))

        return fig
    else:
        if len(op_df) > 1:
            
            op_srcs = []
            for index, row in op_df.iterrows():
                op_srcs.append(Decimal(row.loc["src"]))
            
            max_op_id = op_df.iloc[ op_df['src'].astype(float).idxmax() ]
            min_op_id = op_df.iloc[ op_df['src'].astype(float).idxmin() ]
        else:
            max_op_id = op_df["id"].iloc[0] if len(op_df) > 0 else 1
            min_op_id = op_df["id"].iloc[0] if len(op_df) > 0 else 1
            
        fig = make_subplots(rows=1, cols=1)
        
        for _, row in op_df.iterrows():
            message_ids = [int(x) for x in row["messages"].split("|") if len(x) > 0]
            msg_dsts = list((msg_df.loc[ msg_df["id"].isin(message_ids) ])["dst"])
            
            # ? op_id is the id of the op so that we can plot multiple operations by using the op_id on the y-axis
            # ? op_src is the src node (node all the way on the left)
            # ? msg_dsts is the list of nodes that have received from the op_src (nodes on the line in an ordered fashion)

            x = [row["src"]] + msg_dsts
            y = [row["id"]] * len(x)

            fig.add_trace(
                go.Scatter(
                    x = x, 
                    y = y, 
                    
                    marker=dict(size = 50)
                )
            )
            
            fig.update_xaxes(type="log")
            fig.update_layout(
                yaxis_range=[min_op_id, max_op_id],
                showlegend=False,
                margin=dict(t=0, r=0)
            )

        return fig

if __name__ == "__main__":
    app.run(debug=True)