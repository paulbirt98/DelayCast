import pandas as pd
import matplotlib as plt


import pandas as pd
import matplotlib.pyplot as plt

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
    plt.title(f'Delay Classification by {binned_feature.replace('_', ' ').title()}')
    plt.legend(title=target_col, bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.grid(axis='y', linestyle='--', linewidth=0.5)
    
    if filepath:
        plt.savefig(filepath)
    else:
        plt.show()

    