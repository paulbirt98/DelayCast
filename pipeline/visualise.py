import pandas as pd
from pipeline_utils.config import INDIVIDUAL_ROUTES
import plotly.graph_objects as go

import pandas as pd
import plotly.graph_objects as go
import pandas as pd
import plotly.graph_objects as go

def plot_delay_vs_temperature(df, temp_col='snow_depth', target='delay_classification', bins=5):
    """
    Visualizes how the % of delay classes changes across temperature bins (stacked area, 0–100%).
    """
    df = df.copy()

    # Create temperature bins (intervals)
    df['temp_bin'] = pd.cut(df[temp_col], bins=bins)
    categories = df['temp_bin'].cat.categories  # IntervalIndex

    # % distribution per delay class in each bin
    dist = (
        df.groupby('temp_bin', observed=True)[target]
          .value_counts(normalize=True)
          .rename('percentage')
          .mul(100)
          .reset_index()
    )

    # Pivot to wide format; ensure rows are ordered by the original bin categories
    pivot = (
        dist.pivot_table(index='temp_bin', columns=target, values='percentage', fill_value=0)
            .reindex(categories)
    )

    # X axis = bin midpoints (now definitely an IntervalIndex)
    x = categories.mid

    # Plotly stacked area
    fig = go.Figure()
    for cls in pivot.columns:
        fig.add_trace(go.Scatter(
            x=x,
            y=pivot[cls],
            mode='lines',
            stackgroup='one',
            name=str(cls)
        ))

    fig.update_layout(
        title="Delay Class % vs snow depth",
        xaxis_title="snow depth",
        yaxis_title="Percentage (%)",
        yaxis=dict(range=[0, 100]),
        hovermode="x unified"
    )
    fig.show()



dataset = pd.read_csv(INDIVIDUAL_ROUTES / 'eus_liv_route.csv')
plot_delay_vs_temperature(dataset)
