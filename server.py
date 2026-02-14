"""
Example usage of the Kerala Court Data MCP Server

This shows how to use the MCP tools to enter data for a court.
Run the server first, then use these tools through your MCP client.
"""

# Example: Complete data entry for FTSC Attingal, January 2025

# Step 1: List available FTSC courts
# list_ftsc()
# Output shows: "FTSC Attingal - Thiruvananthapuram (ID: 1)"

# Step 2: Check if data exists
# check_data_exists("FTSC Attingal", 1, 2025)
# Output: "✗ No data found for FTSC Attingal - 1/2025"

# Step 3: Enter basic metrics
"""
insert_step1_basic_metrics(
    court_name="FTSC Attingal",
    month=1,
    year=2025,
    balance_rape=50,
    balance_pocso=30,
    new_rape=5,
    new_pocso=3,
    disposed_rape=4,
    disposed_pocso=2
)
"""
# System calculates and returns:
# - Pending RAPE: 51 (50 + 5 - 4)
# - Pending POCSO: 31 (30 + 3 - 2)
# - Pending TOTAL: 82
# - Balance TOTAL: 80, New TOTAL: 8, Disposed TOTAL: 6

# Step 4: Enter age-wise breakdowns
"""
insert_step2_age_breakdowns(
    court_name="FTSC Attingal",
    month=1,
    year=2025,
    # Pendency breakdown (must sum to 51 for rape, 31 for pocso)
    pending_less_2m_rape=10,
    pending_less_2m_pocso=5,
    pending_2_12m_rape=15,
    pending_2_12m_pocso=10,
    pending_12m_5y_rape=20,
    pending_12m_5y_pocso=12,
    pending_beyond_5y_rape=6,  # Total: 51
    pending_beyond_5y_pocso=4,  # Total: 31
    # Disposal breakdown (must sum to 4 for rape, 2 for pocso)
    disposal_within_2m_rape=1,
    disposal_within_2m_pocso=1,
    disposal_2_12m_rape=2,
    disposal_2_12m_pocso=1,
    disposal_beyond_12m_rape=1,  # Total: 4
    disposal_beyond_12m_pocso=0   # Total: 2
)
"""
# System validates sums and confirms all match

# Step 5: Complete with additional metrics
"""
insert_step3_additional_metrics(
    court_name="FTSC Attingal",
    month=1,
    year=2025,
    contested_rape=3,
    contested_pocso=2,
    disposal_5y_rape=3,
    disposal_5y_pocso=2,
    pending_over_5y_rape=6,
    pending_over_5y_pocso=4,
    convictions_rape=2,
    convictions_pocso=1
)
"""
# System saves to database and confirms success

# Step 6: Query the FTSC view
"""
query_ftsc_view(district_name="Thiruvananthapuram", month=1, year=2025)
"""
# Shows aggregated data for all FTSC courts in Thiruvananthapuram for Jan 2025

# Step 7: If you made a mistake, delete and re-enter
"""
delete_monthly_data("FTSC Attingal", 1, 2025)
"""
# Then start again from Step 3
