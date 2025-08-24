import json
from pathlib import Path
import joblib
import pandas as pd
import numpy as np

class DelayRiskModel:
    def __init__(self, model_dir: Path):
        """
        Load the calibrated model along with featuers and class labels
        """
        model_dir = Path(model_dir)
        self.model = joblib.load(model_dir / "model.joblib")
        self.feature_cols = json.loads((model_dir / "feature_cols.json").read_text())
        self.classes = json.loads((model_dir / "classes.json").read_text())

    def _featurize(self, row: dict) -> pd.DataFrame:
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

    def predict_proba(self, row: dict) -> dict:
        """
        predicts the probabilities of each class - returned in json format
        """
        X = self._featurize(row)
        probs = self.model.predict_proba(X)[0]
        return {cls: float(p) for cls, p in zip(self.classes, probs)}