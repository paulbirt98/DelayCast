import pandas as pd
from pipeline_utils.config import NUMERICAL_FEATURES
from sklearn import SelectKBest, f_classif

def run_anova_f(stoppings_df):
    """
    Runs Anova F tests on all numerical features for the given database.

    Args:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station

    Returns:
    -
    """
    #define features and target
    features = stoppings_df[[NUMERICAL_FEATURES]]
    target = stoppings_df[['delay_classification']]

    #apply anova f tests
    selector = SelectKBest(score=f_classif, k='all')
    selector.fit(features, target)

    #put results to a dataframe
    anova_f_results = pd.DataFrame({
        'Feature': features.columns,
        'F_score': selector.scores_
    }).sort_values(by='F_score', ascending=False)

    print('Anova Scores')
    print(anova_f_results)