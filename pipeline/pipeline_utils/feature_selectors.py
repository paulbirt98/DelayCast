import pandas as pd
from pipeline_utils.config import NUMERICAL_FEATURES, CATEGORICAL_FEATURES
import numpy as np
import seaborn as sb
import matplotlib.pyplot as plot
from sklearn.feature_selection import (
    SelectKBest, 
    f_classif,
    mutual_info_classif,
    chi2
)
from sklearn.preprocessing import LabelEncoder, OrdinalEncoder

def multicoll_heatmap(stoppings_df):
    """
    Produces a correlation matrix for the given NUMERICAL_FEATURES.

    Args:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station
    
    """
    features = stoppings_df[NUMERICAL_FEATURES].dropna() # just in case any unexpected nulls

    #produce the correlation matrix
    matrix = features.corr()

    #plot the heatmap using matplotlib
    plot.figure(figsize=(12, 8))
    heatmap = sb.heatmap(matrix, annot=True, fmt=".2f", cmap='coolwarm', square=True, linewidths=0.5)
    plot.title("Correlation Heatmap of Multicollinearity")
    plot.xticks(rotation=45, ha='right')  
    plot.tight_layout()                   

    fig = plot.gcf()

    return plot.gcf()

def run_anova_f(stoppings_df, numerical_features, target):
    """
    Runs Anova F tests on all numerical features for the given database.

    Args:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station
    - numerical_features (list): a list containing the names of each numerical features as found in the stoppings_df dataframe
    - target (str): the name of the target variable as found in the stoppings_df dataframe

    Returns:
    - anova_f_results (dataframe): The Anova F test results for each feature
    """
    #define features and target
    features = stoppings_df[numerical_features]

    #define target based on input
    if target == 'classification':
        target = stoppings_df['delay_classification']
    elif target == 'minutes':
        target = stoppings_df['delay_minutes']
    else:
        print('Error: Target must be either "classification" or "minutes')

    #apply anova f tests
    selector = SelectKBest(score_func=f_classif, k='all')
    selector.fit(features, target)

    #put results to a dataframe
    anova_f_results = pd.DataFrame({
        'Feature': features.columns,
        'F_score': selector.scores_,
        'p_value': selector.pvalues_
    }).sort_values(by='F_score', ascending=False)

    return anova_f_results

def run_chi_squared(stoppings_df, categorical_features):
    """
    Runs Chi Squared tests on all categorical features for the given database.

    Args:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station
    - categorical_features (list): a list containing the names of each categorical features as found in the stoppings_df dataframe

    Returns:
    - chi2_results (dataframe): The Chi Squared test results for each feature
    """
    #encode categorical features
    x = stoppings_df[categorical_features].copy()
    y = stoppings_df['delay_classification']

    encoder = OrdinalEncoder()
    x_encoded = pd.DataFrame(encoder.fit_transform(x), columns=x.columns)

    #fit
    selector = SelectKBest(score_func=chi2, k='all')
    selector.fit(x_encoded, y)

    scores = selector.scores_
    chi2_results = pd.DataFrame({
        'feature': x.columns,
        'chi_squared_score': scores
    }).sort_values(by='chi_squared_score', ascending=False)

    return chi2_results

def run_mutual_info(stoppings_df, numerical_features, categorical_features):
    """
    Runs Mutual Information tests on all features for the given database.

    Args:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station
    - numerical_features (list): a list containing the names of each numerical features as found in the stoppings_df dataframe
    - categorical_features (list): a list containing the names of each categorical features as found in the stoppings_df dataframe

    Returns:
    - mutual_info_results (dataframe): The Mutual Information test results for each feature
    """
    #define numerical features
    numerical_df = stoppings_df[numerical_features]
    #encode categorical ones
    categorical_df = stoppings_df[categorical_features].copy()

    encoder = OrdinalEncoder()
    categorical_features_encoded = pd.DataFrame(
    encoder.fit_transform(categorical_df),
    columns=categorical_features
    )

                                          
    features = pd.concat([numerical_df, categorical_features_encoded], axis=1)

    #define target
    target = stoppings_df['delay_classification']

    mutual_info_scores = mutual_info_classif(features, target, random_state=0)

    mutual_info_results = pd.DataFrame({
        'feature': features.columns,
        'mutual_info_score': mutual_info_scores
    }).sort_values(by='mutual_info_score', ascending=False)

    return mutual_info_results