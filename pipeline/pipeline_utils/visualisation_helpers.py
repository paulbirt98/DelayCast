import pandas as pd
import matplotlib as plt
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter

def plot_stacked_delay_by_binned_feature(df, feature, bins, labels, target_col='delay_classification', filepath=None):
    """
    Bins a continuous feature and plots a stacked bar chart showing 
    the percentage of each delay classification per bin.

    Parameters:
    - df: DataFrame containing the data
    - feature: name of the continuous feature to bin (e.g., 'rain')
    - bins: list of bin edges
    - labels: list of labels for each bin
    - target_col: name of the delay classification column (default: 'delay_classification')
    """
    # Bin the feature
    binned_feature = f"{feature}_bin"
    df[binned_feature] = pd.cut(df[feature], bins=bins, labels=labels, include_lowest=True)

    # Count delay classifications per bin
    counts = df.groupby([binned_feature, target_col], observed=False).size().unstack(fill_value=0)


    # Convert to percentages
    percent = counts.div(counts.sum(axis=1), axis=0) * 100

    # Plot
    percent.plot(kind='bar', stacked=True, figsize=(10, 6), colormap='tab10', edgecolor='black')
    plt.ylabel('Percentage of Delay Classification')
    plt.xlabel(binned_feature.replace('_', ' ').title())
    plt.title(f"Delay Classification by {binned_feature.replace('_', ' ').title()}")
    plt.legend(title=target_col, bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.grid(axis='y', linestyle='--', linewidth=0.5)
    
    if filepath:
        plt.savefig(filepath)
    else:
        plt.show()

def plot_delay_time_period(station_df, station, time_period_col, target_col='delay_classification', filepath=None):
    """
    Plots delays as a line graph for the given station and given time feature column (i.e. hour, day, or month)

    args:
    - df (dataframe): a dataframe of train stoppings
    - station (Str): three letter station code
    - time_peiod_col (str): the name fo the time period column
    """
    #get user friendly labels
    day_names = {0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"}
    month_names = {1:"Jan", 2:"Feb", 3:"Mar", 4:"Apr", 5:"May", 6:"Jun",
                   7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dec"}
    
    #fitler to station
    station_df = station_df[station_df['station'] == station].copy()
    
    #get labels and right order
    if time_period_col == "day":
        station_df["label"] = station_df[time_period_col].astype(int).map(day_names)
        order = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
        x_axis_label = "Day of week"

    elif time_period_col == "hour":
        station_df["label"] = station_df[time_period_col].astype(int).map(lambda hour: f"{int(hour):02d}:00")
        order = [f"{hour:02d}:00" for hour in range(24)]
        x_axis_label = "Hour"

    elif time_period_col == "month":

        station_df["label"] = station_df[time_period_col].astype(int).map(month_names)
        order = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        x_axis_label = "Month"

    #count the values in each delay class per period
    value_counts =  (station_df.groupby(["label", target_col], observed=False).size().unstack(fill_value=0))

    #ensure ony those that appear are plotted
    order = [label for label in order if label in value_counts.index]
    value_counts = value_counts.reindex(order)

    # convert to percentages per row
    row_totals = value_counts.sum(axis=1)
    percent = value_counts.div(row_totals, axis=0) * 100

    axis = percent.plot(kind="line", marker="o", figsize=(10, 5))
    axis.set_xlabel(x_axis_label)
    axis.set_ylabel("Percentage %")
    axis.yaxis.set_major_formatter(PercentFormatter(xmax=100))
    axis.set_ylim(0, 100)
    axis.set_title(f"{station} — {target_col.replace('_',' ').title()} by {x_axis_label.lower()}")
    axis.grid(axis="y", linestyle="--", linewidth=0.5)
    axis.legend(title=target_col, bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()

    if filepath:
        plt.savefig(filepath, dpi=150)
        plt.close()
    else:
        plt.show()



    