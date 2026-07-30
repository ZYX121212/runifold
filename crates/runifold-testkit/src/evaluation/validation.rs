use super::{EvaluationCaseResult, EvaluationError, EvaluationFailureStage};

pub(super) fn ensure_not_empty(field: &'static str, value: &str) -> Result<(), EvaluationError> {
    if value.trim().is_empty() {
        return Err(EvaluationError::EmptyField { field });
    }
    Ok(())
}

pub(super) fn validate_case_metrics(case: &EvaluationCaseResult) -> Result<(), EvaluationError> {
    if case.metrics.is_some()
        && case
            .failures
            .iter()
            .any(|failure| failure.stage == EvaluationFailureStage::Target)
    {
        return Err(EvaluationError::InconsistentReport {
            message: "target failure cannot contain successful execution metrics",
        });
    }
    if let Some(metrics) = &case.metrics {
        metrics.validate()?;
    }
    Ok(())
}

pub(super) fn ensure_ratio(field: &'static str, value: f64) -> Result<(), EvaluationError> {
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(EvaluationError::InvalidRatio { field, value });
    }
    Ok(())
}

pub(super) fn ensure_non_negative(field: &'static str, value: f64) -> Result<(), EvaluationError> {
    if !value.is_finite() || value < 0.0 {
        return Err(EvaluationError::InvalidMetric { field, value });
    }
    Ok(())
}

pub(super) fn ensure_close(
    actual: f64,
    expected: f64,
    message: &'static str,
) -> Result<(), EvaluationError> {
    const REPORT_RATIO_TOLERANCE: f64 = 1e-12;
    if (actual - expected).abs() > REPORT_RATIO_TOLERANCE {
        return Err(EvaluationError::InconsistentReport { message });
    }
    Ok(())
}
