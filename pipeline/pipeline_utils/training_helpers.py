from sklearn.metrics import cohen_kappa_score
import numpy as np
import itertools

def tune_class_thresholds(y_true, probs, class_labels, step=0.05):
    best_thresh = {}
    best_score = -1
    best_combo = None

    # Define a range of thresholds for each class (except 'No Delay' as fallback)
    thresh_grid = {
        'Severe Delay': np.arange(0.1, 0.5, step),
        'Moderate Delay': np.arange(0.1, 0.5, step),
        'Mild Delay': np.arange(0.1, 0.5, step),
    }

    # Try all combinations of thresholds
    for combo in itertools.product(*thresh_grid.values()):
        thresholds = dict(zip(thresh_grid.keys(), combo))
        preds = []

        for prob in probs:
            assigned = False
            for cls, thresh in thresholds.items():
                if prob[class_labels.tolist().index(cls)] > thresh:
                    preds.append(cls)
                    assigned = True
                    break
            if not assigned:
                preds.append(class_labels[np.argmax(prob)])

        score = cohen_kappa_score(y_true, preds, weights='quadratic')
        if score > best_score:
            best_score = score
            best_thresh = thresholds.copy()
            best_combo = combo

    return best_thresh, best_score
