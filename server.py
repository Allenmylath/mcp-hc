"""
Kerala Court Data MCP Server
FastMCP server for managing court monthly data
"""
import os
from mcp.server.fastmcp import FastMCP
from typing import Optional

# Import database and validation modules
from db import (
    db, get_court_by_name, list_ftsc_courts, list_spc_courts, 
    list_all_districts, check_existing_data, get_existing_data,
    insert_court_monthly_data, delete_court_monthly_data,
    query_ftsc_summary, query_spc_data
)
from validators import (
    validate_basic_metrics, validate_age_breakdowns, 
    validate_additional_metrics, validate_month_year, ValidationError
)

# Create FastMCP server
mcp = FastMCP("Kerala Court Data Server")

# Store partial data during multi-step entry
partial_data_store = {}


@mcp.tool()
async def list_districts() -> str:
    """List all Kerala districts in the database"""
    try:
        districts = await list_all_districts()
        if not districts:
            return "No districts found in database"
        
        result = "Kerala Districts:\n\n"
        for d in districts:
            result += f"{d['display_order']}. {d['name']} (ID: {d['id']})\n"
        
        return result
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool()
async def list_ftsc() -> str:
    """List all FTSC (Fast Track Special Court) courts"""
    try:
        courts = await list_ftsc_courts()
        if not courts:
            return "No FTSC courts found"
        
        result = "FTSC Courts:\n\n"
        for c in courts:
            result += f"• {c['name']} - {c['district_name']} (ID: {c['id']})\n"
        
        return result
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool()
async def list_spc() -> str:
    """List all SPC (Special Court) courts"""
    try:
        courts = await list_spc_courts()
        if not courts:
            return "No SPC courts found"
        
        result = "SPC Courts:\n\n"
        for c in courts:
            result += f"• {c['name']} - {c['district_name']} (ID: {c['id']})\n"
        
        return result
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool()
async def get_court_info(court_name: str) -> str:
    """
    Get information about a specific court by name
    
    Args:
        court_name: Exact court name (e.g., "FTSC Attingal", "SPC TVM")
    """
    try:
        court = await get_court_by_name(court_name)
        if not court:
            return f"Court '{court_name}' not found. Use list_ftsc() or list_spc() to see available courts."
        
        return (f"Court: {court['name']}\n"
                f"Type: {court['type']}\n"
                f"District: {court['district_name']}\n"
                f"Court ID: {court['id']}")
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool()
async def check_data_exists(court_name: str, month: int, year: int) -> str:
    """
    Check if monthly data already exists for a court
    
    Args:
        court_name: Court name (e.g., "FTSC Attingal")
        month: Month (1-12)
        year: Year (e.g., 2025)
    """
    try:
        court = await get_court_by_name(court_name)
        if not court:
            return f"Court '{court_name}' not found"
        
        exists = await check_existing_data(court['id'], month, year)
        
        if exists:
            return f"✓ Data EXISTS for {court_name} - {month}/{year}"
        else:
            return f"✗ No data found for {court_name} - {month}/{year}"
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool()
async def insert_step1_basic_metrics(
    court_name: str,
    month: int,
    year: int,
    balance_rape: float,
    balance_pocso: float,
    new_rape: float,
    new_pocso: float,
    disposed_rape: float,
    disposed_pocso: float
) -> str:
    """
    STEP 1: Insert basic case flow metrics. System will calculate pending and totals.
    
    Args:
        court_name: Court name (e.g., "FTSC Attingal")
        month: Month (1-12)
        year: Year
        balance_rape: Opening balance for RAPE cases
        balance_pocso: Opening balance for POCSO cases
        new_rape: New RAPE cases filed
        new_pocso: New POCSO cases filed
        disposed_rape: RAPE cases disposed
        disposed_pocso: POCSO cases disposed
    """
    try:
        # Validate month/year
        validate_month_year(month, year)
        
        # Get court
        court = await get_court_by_name(court_name)
        if not court:
            return f"Error: Court '{court_name}' not found"
        
        # Check if data already exists
        exists = await check_existing_data(court['id'], month, year)
        if exists:
            return f"Error: Data already exists for {court_name} - {month}/{year}. Delete it first if you want to re-enter."
        
        # Validate and calculate
        calculated = validate_basic_metrics(
            balance_rape, balance_pocso, new_rape, new_pocso, 
            disposed_rape, disposed_pocso
        )
        
        # Store in partial data
        key = f"{court['id']}_{month}_{year}"
        partial_data_store[key] = {
            'court_id': court['id'],
            'court_name': court_name,
            'month': month,
            'year': year,
            **calculated,
            'step1_complete': True
        }
        
        result = f"✓ STEP 1 COMPLETE for {court_name} - {month}/{year}\n\n"
        result += "Calculated values:\n"
        result += f"  Pending RAPE: {calculated['pending_rape']}\n"
        result += f"  Pending POCSO: {calculated['pending_pocso']}\n"
        result += f"  Pending TOTAL: {calculated['pending_total']}\n\n"
        result += f"  Balance TOTAL: {calculated['balance_total']}\n"
        result += f"  New TOTAL: {calculated['new_total']}\n"
        result += f"  Disposed TOTAL: {calculated['disposed_total']}\n\n"
        result += "Next: Use insert_step2_age_breakdowns() to enter age-wise data"
        
        return result
        
    except ValidationError as e:
        return f"Validation Error: {str(e)}"
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool()
async def insert_step2_age_breakdowns(
    court_name: str,
    month: int,
    year: int,
    pending_less_2m_rape: float,
    pending_less_2m_pocso: float,
    pending_2_12m_rape: float,
    pending_2_12m_pocso: float,
    pending_12m_5y_rape: float,
    pending_12m_5y_pocso: float,
    pending_beyond_5y_rape: float,
    pending_beyond_5y_pocso: float,
    disposal_within_2m_rape: float,
    disposal_within_2m_pocso: float,
    disposal_2_12m_rape: float,
    disposal_2_12m_pocso: float,
    disposal_beyond_12m_rape: float,
    disposal_beyond_12m_pocso: float
) -> str:
    """
    STEP 2: Insert age-wise pendency and disposal breakdowns. System will validate sums.
    
    Args:
        court_name: Court name
        month: Month (1-12)
        year: Year
        pending_less_2m_rape: RAPE cases pending < 2 months
        pending_less_2m_pocso: POCSO cases pending < 2 months
        pending_2_12m_rape: RAPE cases pending 2-12 months
        pending_2_12m_pocso: POCSO cases pending 2-12 months
        pending_12m_5y_rape: RAPE cases pending 1-5 years
        pending_12m_5y_pocso: POCSO cases pending 1-5 years
        pending_beyond_5y_rape: RAPE cases pending > 5 years
        pending_beyond_5y_pocso: POCSO cases pending > 5 years
        disposal_within_2m_rape: RAPE disposals within 2 months
        disposal_within_2m_pocso: POCSO disposals within 2 months
        disposal_2_12m_rape: RAPE disposals 2-12 months
        disposal_2_12m_pocso: POCSO disposals 2-12 months
        disposal_beyond_12m_rape: RAPE disposals > 12 months
        disposal_beyond_12m_pocso: POCSO disposals > 12 months
    """
    try:
        # Get court
        court = await get_court_by_name(court_name)
        if not court:
            return f"Error: Court '{court_name}' not found"
        
        # Check if step 1 was completed
        key = f"{court['id']}_{month}_{year}"
        if key not in partial_data_store or not partial_data_store[key].get('step1_complete'):
            return "Error: You must complete STEP 1 (insert_step1_basic_metrics) first"
        
        partial_data = partial_data_store[key]
        
        # Validate age breakdowns
        age_data = validate_age_breakdowns(
            partial_data['pending_rape'],
            partial_data['pending_pocso'],
            partial_data['disposed_rape'],
            partial_data['disposed_pocso'],
            pending_less_2m_rape, pending_less_2m_pocso,
            pending_2_12m_rape, pending_2_12m_pocso,
            pending_12m_5y_rape, pending_12m_5y_pocso,
            pending_beyond_5y_rape, pending_beyond_5y_pocso,
            disposal_within_2m_rape, disposal_within_2m_pocso,
            disposal_2_12m_rape, disposal_2_12m_pocso,
            disposal_beyond_12m_rape, disposal_beyond_12m_pocso
        )
        
        # Update partial data
        partial_data_store[key].update(age_data)
        partial_data_store[key]['step2_complete'] = True
        
        result = f"✓ STEP 2 COMPLETE for {court_name} - {month}/{year}\n\n"
        result += "Validated:\n"
        result += f"  Total Pendency RAPE: {age_data['total_pendency_rape']} ✓\n"
        result += f"  Total Pendency POCSO: {age_data['total_pendency_pocso']} ✓\n"
        result += f"  Total Disposal RAPE: {age_data['total_disposal_rape']} ✓\n"
        result += f"  Total Disposal POCSO: {age_data['total_disposal_pocso']} ✓\n\n"
        result += "Next: Use insert_step3_additional_metrics() to complete data entry"
        
        return result
        
    except ValidationError as e:
        return f"Validation Error: {str(e)}"
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool()
async def insert_step3_additional_metrics(
    court_name: str,
    month: int,
    year: int,
    contested_rape: float = 0,
    contested_pocso: float = 0,
    disposal_5y_rape: float = 0,
    disposal_5y_pocso: float = 0,
    pending_over_5y_rape: float = 0,
    pending_over_5y_pocso: float = 0,
    convictions_rape: float = 0,
    convictions_pocso: float = 0
) -> str:
    """
    STEP 3 (FINAL): Insert additional metrics and save all data to database.
    
    Args:
        court_name: Court name
        month: Month (1-12)
        year: Year
        contested_rape: Contested RAPE cases (default 0)
        contested_pocso: Contested POCSO cases (default 0)
        disposal_5y_rape: RAPE disposals within 5 years (default 0)
        disposal_5y_pocso: POCSO disposals within 5 years (default 0)
        pending_over_5y_rape: RAPE pending over 5 years (default 0)
        pending_over_5y_pocso: POCSO pending over 5 years (default 0)
        convictions_rape: RAPE convictions (default 0)
        convictions_pocso: POCSO convictions (default 0)
    """
    try:
        # Get court
        court = await get_court_by_name(court_name)
        if not court:
            return f"Error: Court '{court_name}' not found"
        
        # Check if steps 1 & 2 were completed
        key = f"{court['id']}_{month}_{year}"
        if key not in partial_data_store:
            return "Error: You must complete STEP 1 first"
        
        if not partial_data_store[key].get('step2_complete'):
            return "Error: You must complete STEP 2 first"
        
        partial_data = partial_data_store[key]
        
        # Validate additional metrics
        additional = validate_additional_metrics(
            partial_data['pending_rape'],
            partial_data['pending_pocso'],
            partial_data['disposed_rape'],
            partial_data['disposed_pocso'],
            contested_rape, contested_pocso,
            disposal_5y_rape, disposal_5y_pocso,
            pending_over_5y_rape, pending_over_5y_pocso,
            convictions_rape, convictions_pocso
        )
        
        # Merge all data
        complete_data = {**partial_data, **additional}
        
        # Insert into database
        await insert_court_monthly_data(court['id'], month, year, complete_data)
        
        # Clean up partial data
        del partial_data_store[key]
        
        result = f"✓✓✓ DATA ENTRY COMPLETE for {court_name} - {month}/{year} ✓✓✓\n\n"
        result += "Successfully saved to database!\n\n"
        result += "Summary:\n"
        result += f"  Balance: {complete_data['balance_total']} ({complete_data['balance_rape']}R + {complete_data['balance_pocso']}P)\n"
        result += f"  New: {complete_data['new_total']} ({complete_data['new_rape']}R + {complete_data['new_pocso']}P)\n"
        result += f"  Disposed: {complete_data['disposed_total']} ({complete_data['disposed_rape']}R + {complete_data['disposed_pocso']}P)\n"
        result += f"  Pending: {complete_data['pending_total']} ({complete_data['pending_rape']}R + {complete_data['pending_pocso']}P)\n"
        result += f"  Convictions: {convictions_rape + convictions_pocso} ({convictions_rape}R + {convictions_pocso}P)\n\n"
        
        # Check if views can be queried
        if court['type'] == 'FTSC':
            result += f"You can now query: query_ftsc_view(district_name='{court['district_name']}')"
        else:
            result += f"You can now query: query_spc_view(court_name='{court_name}')"
        
        return result
        
    except ValidationError as e:
        return f"Validation Error: {str(e)}"
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool()
async def query_ftsc_view(
    district_name: Optional[str] = None,
    month: Optional[int] = None,
    year: Optional[int] = None
) -> str:
    """
    Query the FTSC District Summary view
    
    Args:
        district_name: Filter by district (optional)
        month: Filter by month 1-12 (optional)
        year: Filter by year (optional)
    """
    try:
        results = await query_ftsc_summary(district_name, month, year)
        
        if not results:
            return "No FTSC data found matching your criteria"
        
        output = f"FTSC District Summary ({len(results)} records):\n\n"
        
        for r in results:
            output += f"District: {r['district_name']} | {r['month']}/{r['year']}\n"
            output += f"  Balance: {r['balance_total']} ({r['balance_rape']}R + {r['balance_pocso']}P)\n"
            output += f"  New: {r['new_total']} ({r['new_rape']}R + {r['new_pocso']}P)\n"
            output += f"  Disposed: {r['disposed_total']} ({r['disposed_rape']}R + {r['disposed_pocso']}P)\n"
            output += f"  Pending: {r['pending_total']} ({r['pending_rape']}R + {r['pending_pocso']}P)\n"
            output += f"  Convictions: {r['convictions_rape'] + r['convictions_pocso']} ({r['convictions_rape']}R + {r['convictions_pocso']}P)\n"
            output += f"  Pending >5yr: {r['pending_over_5y_total']}\n"
            output += "-" * 60 + "\n"
        
        return output
        
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool()
async def query_spc_view(
    court_name: Optional[str] = None,
    district_name: Optional[str] = None,
    month: Optional[int] = None,
    year: Optional[int] = None
) -> str:
    """
    Query the SPC Court Data view
    
    Args:
        court_name: Filter by court name (optional)
        district_name: Filter by district (optional)
        month: Filter by month 1-12 (optional)
        year: Filter by year (optional)
    """
    try:
        results = await query_spc_data(court_name, district_name, month, year)
        
        if not results:
            return "No SPC data found matching your criteria"
        
        output = f"SPC Court Data ({len(results)} records):\n\n"
        
        for r in results:
            output += f"Court: {r['court_name']} | District: {r['district_name']} | {r['month']}/{r['year']}\n"
            output += f"  Balance: {r['balance_total']} ({r['balance_rape']}R + {r['balance_pocso']}P)\n"
            output += f"  New: {r['new_total']} ({r['new_rape']}R + {r['new_pocso']}P)\n"
            output += f"  Disposed: {r['disposed_total']} ({r['disposed_rape']}R + {r['disposed_pocso']}P)\n"
            output += f"  Pending: {r['pending_total']} ({r['pending_rape']}R + {r['pending_pocso']}P)\n"
            output += f"  Convictions: {r['convictions_rape'] + r['convictions_pocso']} ({r['convictions_rape']}R + {r['convictions_pocso']}P)\n"
            output += f"  Pending >5yr: {r['pending_over_5y_total']}\n"
            output += "-" * 60 + "\n"
        
        return output
        
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool()
async def delete_monthly_data(court_name: str, month: int, year: int) -> str:
    """
    Delete monthly data for a court (use if you need to re-enter data)
    
    Args:
        court_name: Court name
        month: Month (1-12)
        year: Year
    """
    try:
        court = await get_court_by_name(court_name)
        if not court:
            return f"Error: Court '{court_name}' not found"
        
        # Check if data exists
        exists = await check_existing_data(court['id'], month, year)
        if not exists:
            return f"No data found to delete for {court_name} - {month}/{year}"
        
        # Delete
        await delete_court_monthly_data(court['id'], month, year)
        
        return f"✓ Deleted data for {court_name} - {month}/{year}"
        
    except Exception as e:
        return f"Error: {str(e)}"


# Initialize database connection on startup
@mcp.on_startup()
async def startup():
    """Initialize database connection"""
    print("🚀 Starting Kerala Court Data MCP Server...")
    print(f"📊 DATABASE_URL: {'✓ Set' if os.getenv('DATABASE_URL') else '✗ NOT SET'}")
    try:
        await db.connect()
        print("✓ Connected to Neon database")
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        raise


@mcp.on_shutdown()
async def shutdown():
    """Close database connection"""
    await db.close()
    print("✓ Database connection closed")


if __name__ == "__main__":
    # Run the server with HTTP transport
    # Render provides PORT environment variable
    port = int(os.getenv("PORT", 8000))
    print(f"🌐 Starting MCP server on http://0.0.0.0:{port}")
    # Bind to 0.0.0.0 to accept external connections on Render
    mcp.run(transport="http", host="0.0.0.0", port=port)
