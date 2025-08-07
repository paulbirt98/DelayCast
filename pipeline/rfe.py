import pandas as pd
from sklearn.feature_selection import RFE
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from pipeline_utils.config import NUMERICAL_FEATURES, CATEGORICAL_FEATURES, INDIVIDUAL_ROUTES, UNIFIED_ROUTES_DIR

import argparse

def argparse_cl_arguments():
    """
    
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--route", type=str, default=None)
    parser.add_argument("--filtered_features", type=str, default=None)
    return parser.parse_args()

if __name__ == '__main__':

    args = argparse_cl_arguments()

    if args.filtered_features:
        filtered_out_features = [f.strip() for f in args.filtered_features.split(',')]
    else:
        filtered_out_features = []

    ##ADD ERROR HANDLING FOR ITEMS NOT IN THE LISTS
    kept_numerical_features = [f for f in NUMERICAL_FEATURES if f not in filtered_out_features]
    kept_categorical_features = [f for f in CATEGORICAL_FEATURES if f not in filtered_out_features]
    
    #if no route given, split unified dataset
    if args.route:
        route = args.route.lower()

        route_filepath = INDIVIDUAL_ROUTES / route / f'{route}_training_data.csv'

        dataset = pd.read_csv(route_filepath)

    else:
        kept_categorical_features = kept_categorical_features + ['route']
        filepath = UNIFIED_ROUTES_DIR / 'unified_training_data.csv'
        dataset = pd.read_csv(filepath)

    #prepare features
    numerical_features = dataset[kept_numerical_features]
    categorical_features = dataset[kept_categorical_features].copy()
    for col in categorical_features.columns:
        categorical_features[col] = LabelEncoder().fit_transform(categorical_features[col].astype(str))

    features = pd.concat([numerical_features, categorical_features], axis=1)

    #encode the target
    target = dataset['delay_classification']
    target = LabelEncoder().fit_transform(target)

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    selector = RFE(estimator=model, n_features_to_select=10)
    selector.fit(features, target)

    feature_ranks = pd.DataFrame({
        'feature': features.columns,
        'ranking': selector.ranking_,
        'selected': selector.support_
    }).sort_values(by='ranking')

    print(feature_ranks)