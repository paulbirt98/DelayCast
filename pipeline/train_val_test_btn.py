import json
import joblib
import pandas as pd
from sklearn.frozen import FrozenEstimator
from sklearn.preprocessing import label_binarize
from sklearn.metrics import log_loss, brier_score_loss
from sklearn.ensemble import RandomForestClassifier
from sklearn.calibration import calibration_curve, CalibratedClassifierCV
from pipeline_utils.config import MODELS_DIR, UNIFIED_ROUTES_DIR, INDIVIDUAL_ROUTES
import numpy as np
import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import log_loss
from pipeline_utils.tvt_helpers import report_logloss_skill, report_brier, expected_calibration_error, per_bin_calibration
import argparse

def parse_cl_arguments():
    """

    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--save_to_web", action="store_true")
    return parser.parse_args()

args = parse_cl_arguments()
save_to_web = args.save_to_web

if __name__ == '__main__':
    
    route = 'btn_bdm'
    #load data
    try:
        train = pd.read_csv(INDIVIDUAL_ROUTES / route / f'{route}_training_data.csv')
        val = pd.read_csv(INDIVIDUAL_ROUTES / route / f'{route}_validation_data.csv')
        test = pd.read_csv(INDIVIDUAL_ROUTES / route / f'{route}_testing_data.csv')
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

    train = train.drop(columns=['route'])
    val = val.drop(columns=['route'])
    test = test.drop(columns=['route'])



    #drop unneeded cols
    drop_cols = ['rid', 'date_x', 'scheduled_time', 'actual_time', 'lc_reason', 
                'is_first_station', 'is_terminus', 'delay_classification', 
                'delay_minutes', 'nearest_hour', 'date_y', 'cloud_cover', 
                'snowfall', 'apparent_temperature', 'soil_temperature_0_to_7cm', 
                'wind_speed_10m', 'dew_point_2m', 'is_day',
                'soil_moisture_0_to_7cm', 'wind_direction_10m', 'toc']

    training_features = train.drop(columns=drop_cols, errors='ignore').fillna(0)
    training_target = train['delay_classification']

    val_features = val.drop(columns=drop_cols, errors='ignore').fillna(0)
    val_target = val['delay_classification']

    testing_features = test.drop(columns=drop_cols, errors='ignore').fillna(0)
    testing_target = test['delay_classification']

    #one-hot encode + align
    training_features = pd.get_dummies(training_features)
    val_features = pd.get_dummies(val_features)
    testing_features = pd.get_dummies(testing_features)

    training_features, val_features = training_features.align(val_features, join='left', axis=1, fill_value=0)
    training_features, testing_features = training_features.align(testing_features, join='left', axis=1, fill_value=0)

    #Train base model
    base_clf = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced', n_jobs=-1)
    base_clf.fit(training_features, training_target)

    #Calibrate using validation set
    calibrator = CalibratedClassifierCV(estimator=FrozenEstimator(base_clf), method="isotonic")
    calibrator.fit(val_features, val_target)

    #Evaluate on test set
    probs = calibrator.predict_proba(testing_features)
    class_labels = calibrator.classes_

    scores = report_logloss_skill(testing_target.values, probs, list(class_labels), training_target.values)

    # encode testing_target consistently with class_labels
    label_to_int = {lbl: i for i, lbl in enumerate(class_labels)}
    testing_target_encoded = testing_target.map(label_to_int).to_numpy()

    # safety check
    if np.isnan(testing_target_encoded).any():
        unseen = set(testing_target.unique()) - set(class_labels)
        raise ValueError(f"Unseen labels in test set: {unseen}")
    
    testing_target_binarized = label_binarize(testing_target_encoded, classes=range(len(class_labels)))

    report_brier(training_target, class_labels, probs, testing_target_encoded, testing_target_binarized)

    #Calibration Curves
    print("\nCalibration Curves:")
    plt.figure(figsize=(12, 8))
    for i, cls in enumerate(class_labels):
        true_prob, pred_prob = calibration_curve(testing_target_binarized[:, i], probs[:, i], n_bins=10, strategy='quantile')
        plt.plot(pred_prob, true_prob, marker='o', label=f'{cls}')
    plt.plot([0, 1], [0, 1], 'k--', label='Perfectly Calibrated')
    plt.title("Calibration Curves")
    plt.xlabel("Predicted Probability")
    plt.ylabel("True Frequency")
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.show()

    #get the ece per class both uniform bins and quantile bins
    ece_uniform  = expected_calibration_error(testing_target_encoded, probs, n_bins=10, strategy="uniform")
    ece_quantile = expected_calibration_error(testing_target_encoded, probs, n_bins=10, strategy="quantile")

    print("\nExpected Calibration Error per class:")
    for cls, uniform_ece, quantile_ece in zip(class_labels, ece_uniform, ece_quantile):
        print(f"{cls}: ECE uniform={uniform_ece:.4f}, quantile={quantile_ece:.4f}")
    
    #calibration per bin
    rows = per_bin_calibration(testing_target_encoded, probs, n_bins=10, strategy="quantile")

    # Print a compact per-class summary
    print("\nPer-bin calibration (quantile bins):")
    for c_idx, cls in enumerate(class_labels):
        class_rows = [row for row in rows if row["class_index"] == c_idx]
        if not class_rows:
            continue
        class_ece = sum(r["contribution"] for r in class_rows)
        print(f"\nClass: {cls} | ECE = {class_ece:.4f}")
        for r in class_rows:
            print(f"  bin[{r['lower_boundary']:.2f}, {r['upper_boundary']:.2f}] "
                f"n={r['count']} bin_weight={r['bin_weight']:.3f} "
                f"bin_confidence={r['bin_confidence']:.3f} actual_frequency={r['bin_actual_freq']:.3f} "
                f"gap={r['gap']:.3f} contribution={r['contribution']:.4f}")

    if save_to_web:
        #save model to web_app
        model_dir = MODELS_DIR / 'btn_bdm'
        model_dir.mkdir(parents=True, exist_ok=True)
        model_save_path = model_dir / 'btn_bdm_model'

        #save model to file
        joblib.dump(calibrator, model_dir / 'btn_bdm_model.joblib', compress=0)

        #features
        feature_columns = list(training_features.columns)
        (model_dir / 'feature_columns.json').write_text(json.dumps(feature_columns))

        #class labels
        class_labels = list(calibrator.classes_)
        (model_dir / 'class_labels.json').write_text(json.dumps(class_labels))

        print('Model saved to web_app')