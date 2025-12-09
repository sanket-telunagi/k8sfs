"""Grafana-style dashboard for Kubernetes filesystem metrics."""

import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from typing import Dict, List, Any
from src.database.db_manager import DatabaseManager, SQLiteManager
from config.logging_config import LoggerFactory

logger = LoggerFactory.get_logger(__name__)


class KubernetesMetricsDashboard:
    """Creates a Grafana-style dashboard for K8s metrics."""

    # Grafana-inspired color palette
    GRAFANA_COLORS = {
        "primary": "#3274D9",
        "success": "#6CCE17",
        "warning": "#FFA500",
        "danger": "#D44747",
        "bg_dark": "#1e1e1e",
        "bg_light": "#2d2d2d",
        "text": "#CCCCCC",
    }

    def __init__(self, db_manager: DatabaseManager = None):
        """Initialize dashboard."""
        self.db_manager = db_manager or SQLiteManager()
        self.app = dash.Dash(__name__, suppress_callback_exceptions=True)
        self._setup_theme()
        self._create_layout()
        self._register_callbacks()
        logger.info("Dashboard initialized")

    def _setup_theme(self) -> None:
        """Setup Grafana-style theme."""
        self.app.index_string = """
        <!DOCTYPE html>
        <html>
            <head>
                {%metas%}
                <title>{%title%}</title>
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                {%favicon%}
                {%css%}
                <style>
                    body {{
                        background-color: {bg_dark};
                        color: {text};
                        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
                        margin: 0;
                        padding: 0;
                    }}
                    .dash-table-container {{
                        background-color: {bg_light};
                    }}
                </style>
            </head>
            <body>
                {%app_entry%}
                <footer>
                    {%config%}
                    {%scripts%}
                    {%renderer%}
                </footer>
            </body>
        </html>
        """.format(
            bg_dark=self.GRAFANA_COLORS["bg_dark"],
            bg_light=self.GRAFANA_COLORS["bg_light"],
            text=self.GRAFANA_COLORS["text"],
        )

    def _create_layout(self) -> None:
        """Create dashboard layout."""
        self.app.layout = html.Div(
            [
                # Header
                html.Div(
                    [
                        html.H1(
                            "Kubernetes Filesystem Monitoring",
                            style={
                                "margin": "0",
                                "color": self.GRAFANA_COLORS["text"],
                                "fontWeight": "bold",
                            },
                        ),
                        html.P(
                            "Real-time monitoring of K8s storage metrics",
                            style={
                                "margin": "5px 0 0 0",
                                "color": self.GRAFANA_COLORS["text"],
                                "opacity": "0.7",
                            },
                        ),
                    ],
                    style={
                        "padding": "20px",
                        "background-color": self.GRAFANA_COLORS["bg_light"],
                        "borderBottom": f"2px solid {self.GRAFANA_COLORS['primary']}",
                    },
                ),
                # Controls
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label(
                                    "Namespace:",
                                    style={"color": self.GRAFANA_COLORS["text"]},
                                ),
                                dcc.Dropdown(
                                    id="namespace-filter",
                                    style={
                                        "backgroundColor": self.GRAFANA_COLORS[
                                            "bg_light"
                                        ],
                                        "color": self.GRAFANA_COLORS["text"],
                                    },
                                    placeholder="Select namespace or view all",
                                ),
                            ],
                            style={"flex": "1", "marginRight": "20px"},
                        ),
                        html.Div(
                            [
                                html.Label(
                                    "Time Range:",
                                    style={"color": self.GRAFANA_COLORS["text"]},
                                ),
                                dcc.Dropdown(
                                    id="time-range-filter",
                                    options=[
                                        {"label": "Last 24 Hours", "value": 24},
                                        {"label": "Last 7 Days", "value": 168},
                                        {"label": "Last 30 Days", "value": 720},
                                    ],
                                    value=24,
                                    style={
                                        "backgroundColor": self.GRAFANA_COLORS[
                                            "bg_light"
                                        ],
                                        "color": self.GRAFANA_COLORS["text"],
                                    },
                                ),
                            ],
                            style={"flex": "1", "marginRight": "20px"},
                        ),
                        html.Button(
                            "Refresh",
                            id="refresh-button",
                            n_clicks=0,
                            style={
                                "padding": "10px 20px",
                                "backgroundColor": self.GRAFANA_COLORS["primary"],
                                "color": "white",
                                "border": "none",
                                "borderRadius": "4px",
                                "cursor": "pointer",
                                "marginTop": "24px",
                            },
                        ),
                    ],
                    style={
                        "display": "flex",
                        "padding": "20px",
                        "background-color": self.GRAFANA_COLORS["bg_dark"],
                        "gap": "10px",
                    },
                ),
                # Stats Cards
                html.Div(
                    id="stats-cards",
                    style={
                        "display": "grid",
                        "gridTemplateColumns": "repeat(auto-fit, minmax(200px, 1fr))",
                        "gap": "20px",
                        "padding": "20px",
                        "background-color": self.GRAFANA_COLORS["bg_dark"],
                    },
                ),
                # Charts
                html.Div(
                    [
                        html.Div(
                            [
                                html.H3(
                                    "Node Capacity Trend",
                                    style={"color": self.GRAFANA_COLORS["text"]},
                                ),
                                dcc.Graph(id="capacity-trend-chart"),
                            ],
                            style={
                                "flex": "1",
                                "minWidth": "500px",
                            },
                        ),
                        html.Div(
                            [
                                html.H3(
                                    "Storage Distribution",
                                    style={"color": self.GRAFANA_COLORS["text"]},
                                ),
                                dcc.Graph(id="storage-distribution-chart"),
                            ],
                            style={
                                "flex": "1",
                                "minWidth": "500px",
                            },
                        ),
                    ],
                    style={
                        "display": "flex",
                        "gap": "20px",
                        "padding": "20px",
                        "background-color": self.GRAFANA_COLORS["bg_dark"],
                        "flexWrap": "wrap",
                    },
                ),
                # Node Details Table
                html.Div(
                    [
                        html.H3(
                            "Node Details",
                            style={"color": self.GRAFANA_COLORS["text"]},
                        ),
                        html.Div(id="nodes-table"),
                    ],
                    style={
                        "padding": "20px",
                        "background-color": self.GRAFANA_COLORS["bg_dark"],
                    },
                ),
                # Auto-refresh interval
                dcc.Interval(id="auto-refresh", interval=60000, n_intervals=0),
            ],
            style={
                "background-color": self.GRAFANA_COLORS["bg_dark"],
                "color": self.GRAFANA_COLORS["text"],
                "minHeight": "100vh",
            },
        )

    def _register_callbacks(self) -> None:
        """Register Dash callbacks for interactivity."""

        @self.app.callback(
            [
                Output("namespace-filter", "options"),
                Output("namespace-filter", "value"),
            ],
            [Input("auto-refresh", "n_intervals")],
        )
        def update_namespace_options(n):
            """Update namespace dropdown options."""
            try:
                end_time = datetime.now()
                start_time = end_time - timedelta(hours=24)
                metrics = self.db_manager.query_metrics(start_time, end_time)

                namespaces = sorted(
                    set(m["namespace"] for m in metrics if m.get("namespace"))
                )
                options = [{"label": ns, "value": ns} for ns in namespaces]

                return options, None
            except Exception as e:
                logger.error(f"Error updating namespaces: {e}")
                return [], None

        @self.app.callback(
            [
                Output("stats-cards", "children"),
                Output("capacity-trend-chart", "figure"),
                Output("storage-distribution-chart", "figure"),
                Output("nodes-table", "children"),
            ],
            [
                Input("refresh-button", "n_clicks"),
                Input("auto-refresh", "n_intervals"),
            ],
            [
                State("namespace-filter", "value"),
                State("time-range-filter", "value"),
            ],
        )
        def update_dashboard(refresh_clicks, n_intervals, selected_namespace, hours):
            """Update all dashboard components."""
            try:
                hours = hours or 24

                # Get metrics from database
                end_time = datetime.now()
                start_time = end_time - timedelta(hours=hours)

                metrics = self.db_manager.query_metrics(
                    start_time, end_time, selected_namespace
                )

                if not metrics:
                    return (
                        [html.P("No data available")],
                        {},
                        {},
                        html.P("No node data available"),
                    )

                # Create stats cards
                stats_cards = self._create_stats_cards(metrics)

                # Create charts
                capacity_chart = self._create_capacity_trend_chart(metrics)
                distribution_chart = self._create_distribution_chart(metrics)
                nodes_table = self._create_nodes_table(metrics)

                return stats_cards, capacity_chart, distribution_chart, nodes_table

            except Exception as e:
                logger.error(f"Error updating dashboard: {e}")
                return (
                    [html.P(f"Error: {str(e)}")],
                    {},
                    {},
                    html.P("Error loading data"),
                )

    def _create_stats_cards(self, metrics: List[Dict]) -> List[html.Div]:
        """Create stat cards."""
        cards = []

        # Total nodes
        unique_nodes = len(set(m["node_name"] for m in metrics))
        cards.append(
            self._stat_card(
                "Total Nodes", str(unique_nodes), self.GRAFANA_COLORS["primary"]
            )
        )

        # Total namespaces
        unique_namespaces = len(set(m["namespace"] for m in metrics))
        cards.append(
            self._stat_card(
                "Total Namespaces",
                str(unique_namespaces),
                self.GRAFANA_COLORS["success"],
            )
        )

        # Latest metrics count
        cards.append(
            self._stat_card(
                "Latest Metrics",
                str(len(metrics)),
                self.GRAFANA_COLORS["warning"],
            )
        )

        return cards

    def _stat_card(self, title: str, value: str, color: str) -> html.Div:
        """Create a stat card."""
        return html.Div(
            [
                html.Div([
                    html.H4(
                        title,
                        style={
                            "margin": "0 0 10px 0",
                            "color": self.GRAFANA_COLORS["text"],
                            "fontSize": "14px",
                            "fontWeight": "normal",
                            "opacity": "0.7",
                        },
                    ),
                    html.H2(
                        value,
                        style={
                            "margin": "0",
                            "color": color,
                            "fontSize": "32px",
                            "fontWeight": "bold",
                        },
                    ),
                ])
            ],
            style={
                "padding": "20px",
                "background-color": self.GRAFANA_COLORS["bg_light"],
                "borderRadius": "4px",
                "borderLeft": f"4px solid {color}",
            },
        )

    def _create_capacity_trend_chart(self, metrics: List[Dict]) -> go.Figure:
        """Create capacity trend chart."""
        fig = go.Figure()

        # Group by node
        nodes = {}
        for metric in metrics:
            node = metric["node_name"]
            if node not in nodes:
                nodes[node] = {"timestamps": [], "capacities": []}
            nodes[node]["timestamps"].append(metric["timestamp"])
            nodes[node]["capacities"].append(metric["total_capacity"])

        # Add traces for each node
        colors = [
            self.GRAFANA_COLORS["primary"],
            self.GRAFANA_COLORS["success"],
            self.GRAFANA_COLORS["warning"],
            self.GRAFANA_COLORS["danger"],
        ]

        for idx, (node, data) in enumerate(nodes.items()):
            fig.add_trace(
                go.Scatter(
                    x=data["timestamps"],
                    y=data["capacities"],
                    mode="lines+markers",
                    name=node,
                    line=dict(color=colors[idx % len(colors)], width=2, shape="spline"),
                )
            )

        fig.update_layout(
            title="Storage Capacity Trend",
            xaxis_title="Time",
            yaxis_title="Capacity",
            plot_bgcolor=self.GRAFANA_COLORS["bg_light"],
            paper_bgcolor=self.GRAFANA_COLORS["bg_light"],
            font=dict(color=self.GRAFANA_COLORS["text"]),
            hovermode="x unified",
        )

        return fig

    def _create_distribution_chart(self, metrics: List[Dict]) -> go.Figure:
        """Create storage distribution chart."""
        latest_metrics = {}
        for metric in metrics:
            node = metric["node_name"]
            if (
                node not in latest_metrics
                or metric["timestamp"] > latest_metrics[node]["timestamp"]
            ):
                latest_metrics[node] = metric

        nodes = [m["node_name"] for m in latest_metrics.values()]
        capacities = [m["total_capacity"] for m in latest_metrics.values()]

        fig = go.Figure(
            data=[
                go.Bar(
                    x=nodes,
                    y=capacities,
                    marker=dict(color=self.GRAFANA_COLORS["primary"]),
                )
            ]
        )

        fig.update_layout(
            title="Current Storage Distribution",
            xaxis_title="Node",
            yaxis_title="Capacity",
            plot_bgcolor=self.GRAFANA_COLORS["bg_light"],
            paper_bgcolor=self.GRAFANA_COLORS["bg_light"],
            font=dict(color=self.GRAFANA_COLORS["text"]),
        )

        return fig

    def _create_nodes_table(self, metrics: List[Dict]) -> html.Table:
        """Create nodes details table."""
        latest_metrics = {}
        for metric in metrics:
            node = metric["node_name"]
            if (
                node not in latest_metrics
                or metric["timestamp"] > latest_metrics[node]["timestamp"]
            ):
                latest_metrics[node] = metric

        rows = [
            html.Tr(
                [
                    html.Th("Node", style={"color": self.GRAFANA_COLORS["text"]}),
                    html.Th("Namespace", style={"color": self.GRAFANA_COLORS["text"]}),
                    html.Th("Capacity", style={"color": self.GRAFANA_COLORS["text"]}),
                    html.Th(
                        "Allocatable", style={"color": self.GRAFANA_COLORS["text"]}
                    ),
                    html.Th("Timestamp", style={"color": self.GRAFANA_COLORS["text"]}),
                ],
                style={"borderBottom": f"1px solid {self.GRAFANA_COLORS['bg_light']}"},
            )
        ]

        for metric in sorted(latest_metrics.values(), key=lambda x: x["node_name"]):
            rows.append(
                html.Tr(
                    [
                        html.Td(
                            metric["node_name"],
                            style={
                                "color": self.GRAFANA_COLORS["text"],
                                "padding": "10px",
                            },
                        ),
                        html.Td(
                            metric["namespace"],
                            style={
                                "color": self.GRAFANA_COLORS["text"],
                                "padding": "10px",
                            },
                        ),
                        html.Td(
                            metric["total_capacity"],
                            style={
                                "color": self.GRAFANA_COLORS["text"],
                                "padding": "10px",
                            },
                        ),
                        html.Td(
                            metric["total_allocatable"],
                            style={
                                "color": self.GRAFANA_COLORS["text"],
                                "padding": "10px",
                            },
                        ),
                        html.Td(
                            metric["timestamp"],
                            style={
                                "color": self.GRAFANA_COLORS["text"],
                                "padding": "10px",
                            },
                        ),
                    ],
                    style={
                        "borderBottom": f"1px solid {self.GRAFANA_COLORS['bg_light']}"
                    },
                )
            )

        return html.Table(
            rows,
            style={
                "width": "100%",
                "borderCollapse": "collapse",
            },
        )

    def run(self, debug: bool = False, port: int = 8050, host: str = "127.0.0.1"):
        """Run the dashboard server."""
        logger.info(f"Starting dashboard on http://{host}:{port}")
        self.app.run_server(debug=debug, port=port, host=host)


def create_dashboard_app(
    db_manager: DatabaseManager = None, db_type: str = "sqlite"
) -> dash.Dash:
    """Factory function to create dashboard app."""
    if not db_manager and db_type == "sqlite":
        db_manager = SQLiteManager()
    elif not db_manager and db_type == "duckdb":
        from src.database.db_manager import DuckDBManager

        try:
            db_manager = DuckDBManager()
        except ImportError:
            logger.warning("DuckDB not available, using SQLite")
            db_manager = SQLiteManager()

    dashboard = KubernetesMetricsDashboard(db_manager)
    return dashboard.app
