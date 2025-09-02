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
    try:
        if route:
            route = route.lower()
            train = pd.read_csv(INDIVIDUAL_ROUTES / route / 'binned' / f'{route}_binned_training_data.csv')
            val = pd.read_csv(INDIVIDUAL_ROUTES / route / 'binned' / f'{route}_binned_validation_data.csv')
            test = pd.read_csv(INDIVIDUAL_ROUTES / route / 'binned' / f'{route}_binned_testing_data.csv')

        else:
            route = 'unified_routes'
            train = pd.read_csv(UNIFIED_ROUTES_DIR / f'{route}_training_data.csv')
            val = pd.read_csv(UNIFIED_ROUTES_DIR / f'{route}_validation_data.csv')
            test = pd.read_csv(UNIFIED_ROUTES_DIR / f'{route}_testing_data.csv')

            #remove btnbdm
            train = train[train['route'] != 'btn_bdm']
            val = val[val['route'] != 'btn_bdm']
            test = test[test['route'] != 'btn_bdm']
    except FileNotFoundError:
        print(f"Error: File not found")
        raise
    except pd.errors.ParserError:
        print(f"Error parsing file")
        raise
    except PermissionError:
        print(f"Permission Error with file. Ensure the file is not open elsewhere.")
        raise
    except Exception as e:
        print(f"Unexpected error reading file: {e}")

    # drop all non-feature columns
    drop_cols = ['rid', 'date_x', 'scheduled_time', 'actual_time', 'lc_reason', 'delay_classification', 
                'delay_minutes', 'nearest_hour', 'date_y', 'cloud_cover', 'apparent_temperature', 'soil_temperature_0_to_7cm', 
                'wind_speed_10m', 'dew_point_2m', 'is_day', 'wind_direction_10m', 'toc']

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
    smote_targets = {
        'Mild Delay': 80000,
        'Moderate Delay': 50000,
        'Severe Delay': 25000
    }
    smote = SMOTE(random_state=42, sampling_strategy=smote_targets, k_neighbors=5)
    X_train_resampled, y_train_resampled = smote.fit_resample(X_train, y_train)
    

    print("SMOTE class distribution:")
    print(pd.Series(y_train_resampled).value_counts())
    """

    # Train hte base model
    base_clf = RandomForestClassifier(n_estimators=100, random_state=42, min_samples_leaf=50, n_jobs=-1)
    base_clf.fit(X_train, y_train) #if not using smote
    #base_clf.fit(X_train_resampled, y_train_resampled) # if using SMOTE

    # calibrate using calibrated classifier using val set
    calibrator = CalibratedClassifierCV(estimator=base_clf, method='isotonic', cv='prefit')
    calibrator.fit(X_val, y_val)

    # evaluate on the test data
    probs = calibrator.predict_proba(X_test)
    class_labels = calibrator.classes_
    
    y_test_bin = label_binarize(y_test, classes=class_labels)

    # log loss results
    logloss = log_loss(y_test, probs, labels=class_labels)
    print(f"\nLog Loss: {logloss:.4f}")

    # brier results
    print("\nBrier Scores per class:")
    for i, cls in enumerate(class_labels):
        if y_test_bin[:, i].sum() == 0:
            print(f"{cls}: skipped (no positives in test set)")
            continue
        brier = brier_score_loss(y_test_bin[:, i], probs[:, i])
        print(f"{cls}: {brier:.4f}")

    # plot claibration curves
    print("\nCalibration Curves:")
    plt.figure(figsize=(12, 8))
    for i, cls in enumerate(class_labels):
        pos = int(y_test_bin[:, i].sum())
        neg = y_test_bin.shape[0] - pos
        if pos == 0 or neg == 0:
            continue
        true_prob, pred_prob = calibration_curve(y_test_bin[:, i], probs[:, i], n_bins=10, strategy='quantile')
        plt.plot(pred_prob, true_prob, marker='o', label=f'{cls}')
    plt.plot([0, 1], [0, 1], 'k--', label='Perfectly Calibrated')
    plt.title("Calibration Curves")
    plt.xlabel("Predicted Probability")
    plt.ylabel("True Frequency")
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.show()

    
