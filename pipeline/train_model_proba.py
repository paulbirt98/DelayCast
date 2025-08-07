import pandas as pd
from sklearn.preprocessing import LabelEncoder, label_binarize
from sklearn.metrics import log_loss, brier_score_loss
from sklearn.ensemble import RandomForestClassifier
from sklearn.calibration import calibration_curve, CalibratedClassifierCV
from pipeline_utils.config import UNIFIED_ROUTES_DIR, INDIVIDUAL_ROUTES
import numpy as np
import matplotlib.pyplot as plt
import argparse
from imblearn.over_sampling import SMOTE

def argparse_cl_arguments():
    """
    
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--route", type=str, default=None)
    return parser.parse_args()

if __name__ == '__main__':

    args = argparse_cl_arguments()
    route = args.route

    #load either route or unified dataset
    if route:
        route = route.lower()
        train = pd.read_csv(INDIVIDUAL_ROUTES / route / f'{route}_training_data.csv')
        val = pd.read_csv(INDIVIDUAL_ROUTES / route / f'{route}_validation_data.csv')
        test = pd.read_csv(INDIVIDUAL_ROUTES / route / f'{route}_testing_data.csv')

    else:
        train = pd.read_csv(UNIFIED_ROUTES_DIR / 'unified_training_data.csv')
        val = pd.read_csv(UNIFIED_ROUTES_DIR / 'unified_validation_data.csv')
        test = pd.read_csv(UNIFIED_ROUTES_DIR / 'unified_testing_data.csv')

    # drop all non-feature columns
    drop_cols = ['rid', 'date_x', 'scheduled_time', 'actual_time', 'lc_reason', 
                'is_first_station', 'is_terminus', 'delay_classification', 
                'delay_minutes', 'nearest_hour', 'date_y', 'cloud_cover', 
                'snowfall', 'apparent_temperature', 'soil_temperature_0_to_7cm', 
                'wind_speed_10m', 'dew_point_2m', 'is_day',
                'soil_moisture_0_to_7cm', 'wind_direction_10m', 'toc']

    X_train = train.drop(columns=drop_cols, errors='ignore').fillna(0)
    y_train = train['delay_classification']

    X_val = val.drop(columns=drop_cols, errors='ignore').fillna(0)
    y_val = val['delay_classification']

    X_test = test.drop(columns=drop_cols, errors='ignore').fillna(0)
    y_test = test['delay_classification']

    #one-hot code the categorical features
    X_train = pd.get_dummies(X_train)
    X_val = pd.get_dummies(X_val)
    X_test = pd.get_dummies(X_test)

    X_train, X_val = X_train.align(X_val, join='left', axis=1, fill_value=0)
    X_train, X_test = X_train.align(X_test, join='left', axis=1, fill_value=0)

    """
    #smote for minority classes
    print("\nSMOTE OVERSAMPLING")
    smote = SMOTE(random_state=42)
    X_train_resampled, y_train_resampled = smote.fit_resample(X_train, y_train)
    

    print("SMOTE class distribution:")
    print(pd.Series(y_train_resampled).value_counts())
    """

    # Train hte base model
    base_clf = RandomForestClassifier(n_estimators=100, random_state=42)
    base_clf.fit(X_train, y_train)
    #base_clf.fit(X_train_resampled, y_train_resampled) # if using SMOTE

    # calibrate using calibrated classifier using val set
    calibrator = CalibratedClassifierCV(estimator=base_clf, method='isotonic', cv='prefit')
    calibrator.fit(X_val, y_val)

    # evaluate on the test data
    probs = calibrator.predict_proba(X_test)
    class_labels = calibrator.classes_
    le = LabelEncoder()
    y_test_encoded = le.fit_transform(y_test)
    y_test_bin = label_binarize(y_test_encoded, classes=range(len(class_labels)))

    # log loss results
    logloss = log_loss(y_test_bin, probs)
    print(f"\nLog Loss: {logloss:.4f}")

    # brier results
    print("\nBrier Scores per class:")
    for i, cls in enumerate(class_labels):
        brier = brier_score_loss(y_test_bin[:, i], probs[:, i])
        print(f"{cls}: {brier:.4f}")

    # plot claibration curves
    print("\nCalibration Curves:")
    plt.figure(figsize=(12, 8))
    for i, cls in enumerate(class_labels):
        true_prob, pred_prob = calibration_curve(y_test_bin[:, i], probs[:, i], n_bins=15, strategy='uniform')
        plt.plot(pred_prob, true_prob, marker='o', label=f'{cls}')
    plt.plot([0, 1], [0, 1], 'k--', label='Perfectly Calibrated')
    plt.title("Calibration Curves")
    plt.xlabel("Predicted Probability")
    plt.ylabel("True Frequency")
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.show()
