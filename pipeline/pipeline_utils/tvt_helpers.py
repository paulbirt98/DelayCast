import math
import numpy as np
from collections import Counter
from sklearn.metrics import log_loss, brier_score_loss

def report_logloss_skill(test_labels, predicted_probabilities, class_labels, training_labels):
    """
    Prints the model's log loss and skill scores against training climatology and uniform.
    Also prints context stats , test set distribution and the unform forecaster log loss.

    Args:
        test_labels   : iterable of true class labels on the TEST set (strings)
        predicted_probabilities : (N x K) array of predicted probabilities, columns align to class_labels
        class_labels    : list of class names in the same order as predicted_proba columns
        training_labels  : iterable of TRAIN labels (used to build climatology baseline)

    Returns:
    - results (dict): a dictionary containing:
        - "logloss" (float): model log loss on the test set
        - "test_entropy" (float): test entripy computed from empirical class proportions
        - "uniform_log_loss" (float): ln(K), the log loss of a uniform predictor
        - "skill_vs_train" (float): 1 - model_logloss / baseline_logloss (climatology skill)
        - "skill_vs_uniform" (float): 1 - model_logloss / uniform_log_loss (uniform skill)
        - "train_priors" (ndarray): length-K array of training-set class proportions

    """
    num_classes = len(class_labels)
    num_samples = len(test_labels)

    if predicted_probabilities.shape[1] != num_classes:
        raise ValueError(
            f"predicted_probabilities has {predicted_probabilities.shape[1]} columns "
            f"but class_labels has {num_classes} entries."
        )

    # get the models log los on the test data
    model_logloss = log_loss(test_labels, predicted_probabilities, labels=class_labels)

    # get the uniform forecaster log loss
    uniform_log_loss = math.log(num_classes)

    #class frequency in test_labels and get proportions
    test_counts = Counter(test_labels)
    test_priors = np.array([test_counts.get(label, 0) / max(num_samples, 1) for label in class_labels], float)
    test_priors = np.clip(test_priors, 1e-12, 1.0); test_priors /= test_priors.sum()

    #calculate the entropy of the test data
    test_entropy = float(-(test_priors * np.log(test_priors)).sum())

    #class frequency in the training data and get proportions
    train_counts = Counter(training_labels)
    train_priors = np.array([train_counts.get(label, 0) / max(len(training_labels), 1) for label in class_labels], float)
    train_priors = np.clip(train_priors, 1e-12, 1.0); train_priors /= train_priors.sum()

    #calculate baseline log loss (if the training data class frequencies are alwasy predicted)
    baseline_matrix = np.repeat(train_priors.reshape(1, -1), num_samples, axis=0)
    baseline_logloss = log_loss(test_labels, baseline_matrix, labels=class_labels)

    # get skil vs training and uniform
    skill_vs_train = 1.0 - (model_logloss / baseline_logloss)
    skill_vs_uniform = 1.0 - (model_logloss / uniform_log_loss)

    #print the reported values
    print(
        f"test_entropy={test_entropy:.4f}, uniform_baseline_logloss={uniform_log_loss:.4f}, model={model_logloss:.4f}, "
        f"skill_vs_train={skill_vs_train:.3f}, skill_vs_uniform={skill_vs_uniform:.3f}"
    )

    return {
        "logloss": model_logloss,
        "test_entropy": test_entropy,
        "uniform_log_loss": uniform_log_loss,
        "skill_vs_train": skill_vs_train,
        "skill_vs_uniform": skill_vs_uniform,
        "train_priors": train_priors,
    }

def report_brier(training_labels, class_labels, predicted_probabilities, testing_target_encoded, testing_target_binarized):
    """
    computes and prints multiclass brier scores: overall (global) brier score,
    a training climatology baseline brier, and brier skill score. also prints per-class
    brier scores and per-class brier skill.

    Args:
    - training_labels (list): training-set class labels
    - class_labels (list[str]): ordered class names corresponding to probability columns
    - predicted_probabilities (array): array with model-predicted class probabilities
    - testing_target_encoded (array[int]): array of encoded class indices
    - testing_target_binarized (array[int]): one-hot matrix of shape for test labels
    """
    train_counts = Counter(training_labels.values)
    train_priors = np.array([train_counts.get(lbl, 0) / len(training_labels) for lbl in class_labels], float)
    train_priors = np.clip(train_priors, 1e-12, 1.0); train_priors /= train_priors.sum()

    #get multiclass brier score
    one_hot_truth_matrix = np.zeros_like(predicted_probabilities)
    one_hot_truth_matrix[np.arange(len(testing_target_encoded)), testing_target_encoded] = 1.0
    brier_multiclass = float(np.mean(np.sum((predicted_probabilities - one_hot_truth_matrix)**2, axis=1)))

    #get the baseline brier score and brier skill
    train_prior_matrix = np.repeat(train_priors.reshape(1, -1), len(testing_target_encoded), axis=0)
    global_brier_baseline = float(np.mean(np.sum((train_prior_matrix - one_hot_truth_matrix)**2, axis=1)))
    global_brier_skill = 1 - brier_multiclass / global_brier_baseline
    print(f"\nGlobal Brier: brier={brier_multiclass:.4f}, baseline={global_brier_baseline:.4f}, brier_skill={global_brier_skill:.3f}")

    #Per-class Brier and per-class Brier skill
    print("\nBrier per class:")
    for i, label in enumerate(class_labels):
        brier_per_class  = brier_score_loss(testing_target_binarized[:, i], predicted_probabilities[:, i])
        brier_per_class_baseline = brier_score_loss(testing_target_binarized[:, i], np.full(len(testing_target_encoded), train_priors[i]))
        brier_per_class_skill = 1 - brier_per_class / brier_per_class_baseline if brier_per_class_baseline > 0 else float('nan')
        print(f"{label}: {brier_per_class:.4f} (skill={brier_per_class_skill:.3f})")

def expected_calibration_error(testing_target_encoded, predicted_probabilities, n_bins=10, strategy="uniform"):
    """
    computes class-wise expected calibration error (ECE).

    Args:
    - testing_target_encoded (array[int]): array of encoded class indices
    - predicted_probabilities (array[float]): array with predicted class probabilities
    - n_bins (int): number of calibration bins; default is 10
    - strategy (str): binning strategy, "uniform" for equal-width bins in [0,1] or
    "quantile" for equal-mass bins based on predicted probabilities; default "uniform"

    Returns:
    - ece_list (list[float]): list of per-class ECE values; 0.0 indicates perfect calibration
    for that class.
    """

    n_classes = predicted_probabilities.shape[1]
    ece_list = []

    for c in range(n_classes):
        true_class = (testing_target_encoded == c).astype(int)
        prob_class = predicted_probabilities[:, c]

        if strategy == "quantile":
            edges = np.quantile(prob_class, np.linspace(0, 1, n_bins+1))
            edges = np.unique(edges) # remove duplicates
            if len(edges) <= 2:                            
                ece_list.append(0.0); continue
        else:
            edges = np.linspace(0, 1, n_bins+1)

        ece = 0.0
        num_samples = len(prob_class)
        for i in range(len(edges)-1):
            low, high = edges[i], edges[i+1]
            
            mask = (prob_class >= low) & (prob_class < high if i < len(edges)-2 else prob_class <= high)
            if not np.any(mask):
                continue
            mean_probability = prob_class[mask].mean()
            actual  = true_class[mask].mean()
            ece += (mask.sum()/num_samples) * abs(mean_probability - actual)
        ece_list.append(ece)
    return ece_list

def per_bin_calibration(testing_target_encoded, predicted_probabilities, n_bins=10, strategy="uniform"):
        """
        Returns per-bin stats for each class:
        - low, high: bin edges
        - count: samples in bin
        - prop: count / N
        - conf: mean predicted prob in bin
        - acc: mean accuracy in bin (for this class as 1-vs-rest)
        - gap: |conf - acc|
        - contrib: prop * gap (bin's contribution to class ECE)
        """
        num_samples = predicted_probabilities.shape[0]
        num_classes = predicted_probabilities.shape[1]
        result = []  # list of dicts, one row per (class, bin)

        for class_index in range(num_classes):
            y_c = (testing_target_encoded == class_index).astype(int)
            p_c = predicted_probabilities[:, class_index]

            if strategy == "quantile":
                edges = np.quantile(p_c, np.linspace(0, 1, n_bins + 1))
                edges = np.unique(edges)
                if len(edges) <= 2:
                    edges = np.array([0.0, 1.0])
            else:
                edges = np.linspace(0, 1, n_bins + 1)

            for i in range(len(edges) - 1):
                low, high = edges[i], edges[i + 1]
                mask = (p_c >= low) & (p_c < high if i < len(edges) - 2 else p_c <= high)
                cnt = int(mask.sum())
                if cnt == 0:
                    continue
                confidence = float(p_c[mask].mean())
                acc  = float(y_c[mask].mean())
                gap = abs(confidence - acc)
                bin_weight = cnt / num_samples
                contribution = bin_weight * gap
                result.append({
                    "class_index": class_index,
                    "lower_boundary": float(low),
                    "upper_boundary": float(high),
                    "count": cnt,
                    "bin_weight": bin_weight,
                    "bin_confidence": confidence,
                    "bin_actual_freq": acc,
                    "gap": gap,
                    "contribution": contribution
                })
        return result