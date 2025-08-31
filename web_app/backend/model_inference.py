import json
from pathlib import Path
import joblib
import pandas as pd

class DelayRiskModel:
    def __init__(self, model_dir):
        """
        Load the calibrated model along with featuers and class labels
        """
        model_dir = Path(model_dir)
        route_name = model_dir.name 
        model_filepath = model_dir / f"{route_name}_model.joblib"


        self.model = joblib.load(model_filepath, mmap_mode='r')
        self.feature_cols = json.loads((model_dir / "feature_columns.json").read_text())
        self.classes = json.loads((model_dir / "class_labels.json").read_text())

    def _featurize(self, row):
        """
        ensure the inputs are in the requried format
        """
        df = pd.DataFrame([row]).fillna(0)
        df = pd.get_dummies(df)

        # add any missing training columns with 0
        for col in self.feature_cols:
            if col not in df.columns:
                df[col] = 0

        # drop any unexpected columns and reorder exactly
        df = df[self.feature_cols]
        return df

    def predict_proba(self, row):
        """
        predicts the probabilities of each class - returned in json format
        """
        X = self._featurize(row)
        probs = self.model.predict_proba(X)[0]
        return {cls: float(p) for cls, p in zip(self.classes, probs)}