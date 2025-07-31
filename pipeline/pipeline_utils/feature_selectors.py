import pandas as pd
from pipeline_utils.config import NUMERICAL_FEATURES
from sklearn.feature_selection import (
    SelectKBest, 
    f_classif,
    mutual_info_classif
)

def run_anova_f(stoppings_df, p_threshold):
    """
    Runs Anova F tests on all numerical features for the given database.

    Args:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station

    Returns:
    - 
    """
    #define features and target
    features = stoppings_df[NUMERICAL_FEATURES]
    target = stoppings_df['delay_classification']

    #apply anova f tests
    selector = SelectKBest(score_func=f_classif, k='all')
    selector.fit(features, target)

    #put results to a dataframe
    anova_f_results = pd.DataFrame({
        'Feature': features.columns,
        'F_score': selector.scores_,
        'p_value': selector.pvalues_
    }).sort_values(by='F_score', ascending=False)

    if p_threshold is not None:
        anova_f_results = anova_f_results[anova_f_results['p_value'] < p_threshold]

    print('Anova Scores')
    print(anova_f_results)

def run_mutual_info(stoppings_df):
    """
    
    """
    #define features and target
    features = stoppings_df[NUMERICAL_FEATURES]
    target = stoppings_df['delay_classification']

    mutual_info_scores = mutual_info_classif(features, target, random_state=0)

    mutual_info_results = pd.DataFrame({
        'feature': features.columns,
        'mutual_info_score': mutual_info_scores
    }).sort_values(by='mutual_info_score', ascending=False)


    print(mutual_info_results)