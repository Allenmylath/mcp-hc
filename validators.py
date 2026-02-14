"""
Validation logic for court monthly data
"""

class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass


def validate_basic_metrics(balance_rape, balance_pocso, new_rape, new_pocso, 
                          disposed_rape, disposed_pocso):
    """
    Validate basic metrics and calculate derived values
    Returns: dict with all calculated values
    """
    # Calculate pending
    pending_rape = balance_rape + new_rape - disposed_rape
    pending_pocso = balance_pocso + new_pocso - disposed_pocso
    
    # Calculate totals
    balance_total = balance_rape + balance_pocso
    new_total = new_rape + new_pocso
    disposed_total = disposed_rape + disposed_pocso
    pending_total = pending_rape + pending_pocso
    
    # Validate pending calculation
    if pending_total != balance_total + new_total - disposed_total:
        raise ValidationError("Pending calculation mismatch")
    
    return {
        'balance_rape': balance_rape,
        'balance_pocso': balance_pocso,
        'balance_total': balance_total,
        'new_rape': new_rape,
        'new_pocso': new_pocso,
        'new_total': new_total,
        'disposed_rape': disposed_rape,
        'disposed_pocso': disposed_pocso,
        'disposed_total': disposed_total,
        'pending_rape': pending_rape,
        'pending_pocso': pending_pocso,
        'pending_total': pending_total
    }


def validate_age_breakdowns(pending_rape, pending_pocso, disposed_rape, disposed_pocso,
                           pending_less_2m_rape, pending_less_2m_pocso,
                           pending_2_12m_rape, pending_2_12m_pocso,
                           pending_12m_5y_rape, pending_12m_5y_pocso,
                           pending_beyond_5y_rape, pending_beyond_5y_pocso,
                           disposal_within_2m_rape, disposal_within_2m_pocso,
                           disposal_2_12m_rape, disposal_2_12m_pocso,
                           disposal_beyond_12m_rape, disposal_beyond_12m_pocso):
    """
    Validate age-wise breakdowns sum correctly
    Returns: dict with calculated total_pendency and total_disposal
    """
    # Calculate pendency sums
    total_pendency_rape = (pending_less_2m_rape + pending_2_12m_rape + 
                          pending_12m_5y_rape + pending_beyond_5y_rape)
    total_pendency_pocso = (pending_less_2m_pocso + pending_2_12m_pocso + 
                           pending_12m_5y_pocso + pending_beyond_5y_pocso)
    
    # Validate pendency sums
    if total_pendency_rape != pending_rape:
        raise ValidationError(
            f"RAPE pendency breakdown sum ({total_pendency_rape}) != "
            f"pending_rape ({pending_rape})"
        )
    
    if total_pendency_pocso != pending_pocso:
        raise ValidationError(
            f"POCSO pendency breakdown sum ({total_pendency_pocso}) != "
            f"pending_pocso ({pending_pocso})"
        )
    
    # Calculate disposal sums
    total_disposal_rape = (disposal_within_2m_rape + disposal_2_12m_rape + 
                          disposal_beyond_12m_rape)
    total_disposal_pocso = (disposal_within_2m_pocso + disposal_2_12m_pocso + 
                           disposal_beyond_12m_pocso)
    
    # Validate disposal sums
    if total_disposal_rape != disposed_rape:
        raise ValidationError(
            f"RAPE disposal breakdown sum ({total_disposal_rape}) != "
            f"disposed_rape ({disposed_rape})"
        )
    
    if total_disposal_pocso != disposed_pocso:
        raise ValidationError(
            f"POCSO disposal breakdown sum ({total_disposal_pocso}) != "
            f"disposed_pocso ({disposed_pocso})"
        )
    
    return {
        'pending_less_2m_rape': pending_less_2m_rape,
        'pending_less_2m_pocso': pending_less_2m_pocso,
        'pending_2_12m_rape': pending_2_12m_rape,
        'pending_2_12m_pocso': pending_2_12m_pocso,
        'pending_12m_5y_rape': pending_12m_5y_rape,
        'pending_12m_5y_pocso': pending_12m_5y_pocso,
        'pending_beyond_5y_rape': pending_beyond_5y_rape,
        'pending_beyond_5y_pocso': pending_beyond_5y_pocso,
        'total_pendency_rape': total_pendency_rape,
        'total_pendency_pocso': total_pendency_pocso,
        'disposal_within_2m_rape': disposal_within_2m_rape,
        'disposal_within_2m_pocso': disposal_within_2m_pocso,
        'disposal_2_12m_rape': disposal_2_12m_rape,
        'disposal_2_12m_pocso': disposal_2_12m_pocso,
        'disposal_beyond_12m_rape': disposal_beyond_12m_rape,
        'disposal_beyond_12m_pocso': disposal_beyond_12m_pocso,
        'total_disposal_rape': total_disposal_rape,
        'total_disposal_pocso': total_disposal_pocso
    }


def validate_additional_metrics(pending_rape, pending_pocso, disposed_rape, disposed_pocso,
                                contested_rape, contested_pocso,
                                disposal_5y_rape, disposal_5y_pocso,
                                pending_over_5y_rape, pending_over_5y_pocso,
                                convictions_rape, convictions_pocso):
    """
    Validate subset relationships for additional metrics
    Returns: dict with all additional metrics including calculated totals
    """
    # Validate contested is subset of disposed
    if contested_rape > disposed_rape:
        raise ValidationError(
            f"contested_rape ({contested_rape}) cannot exceed "
            f"disposed_rape ({disposed_rape})"
        )
    
    if contested_pocso > disposed_pocso:
        raise ValidationError(
            f"contested_pocso ({contested_pocso}) cannot exceed "
            f"disposed_pocso ({disposed_pocso})"
        )
    
    # Validate disposal_5y is subset of disposed
    if disposal_5y_rape > disposed_rape:
        raise ValidationError(
            f"disposal_5y_rape ({disposal_5y_rape}) cannot exceed "
            f"disposed_rape ({disposed_rape})"
        )
    
    if disposal_5y_pocso > disposed_pocso:
        raise ValidationError(
            f"disposal_5y_pocso ({disposal_5y_pocso}) cannot exceed "
            f"disposed_pocso ({disposed_pocso})"
        )
    
    # Validate pending_over_5y is subset of pending
    if pending_over_5y_rape > pending_rape:
        raise ValidationError(
            f"pending_over_5y_rape ({pending_over_5y_rape}) cannot exceed "
            f"pending_rape ({pending_rape})"
        )
    
    if pending_over_5y_pocso > pending_pocso:
        raise ValidationError(
            f"pending_over_5y_pocso ({pending_over_5y_pocso}) cannot exceed "
            f"pending_pocso ({pending_pocso})"
        )
    
    # Calculate totals
    contested_total = contested_rape + contested_pocso
    disposal_5y_total = disposal_5y_rape + disposal_5y_pocso
    pending_over_5y_total = pending_over_5y_rape + pending_over_5y_pocso
    
    return {
        'contested_rape': contested_rape,
        'contested_pocso': contested_pocso,
        'contested_total': contested_total,
        'disposal_5y_rape': disposal_5y_rape,
        'disposal_5y_pocso': disposal_5y_pocso,
        'disposal_5y_total': disposal_5y_total,
        'pending_over_5y_rape': pending_over_5y_rape,
        'pending_over_5y_pocso': pending_over_5y_pocso,
        'pending_over_5y_total': pending_over_5y_total,
        'convictions_rape': convictions_rape,
        'convictions_pocso': convictions_pocso
    }


def validate_month_year(month, year):
    """Validate month and year are in valid ranges"""
    if not (1 <= month <= 12):
        raise ValidationError(f"Month must be between 1-12, got {month}")
    
    if not (2020 <= year <= 2030):
        raise ValidationError(f"Year seems invalid: {year}")
    
    return True
