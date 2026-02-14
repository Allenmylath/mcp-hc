"""
Database connection and query functions
"""
import os
import asyncpg
from typing import Optional, List, Dict, Any


class Database:
    """Database connection manager"""
    
    def __init__(self):
        self.pool = None
    
    async def connect(self):
        """Create connection pool"""
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            raise ValueError("DATABASE_URL environment variable not set")
        
        self.pool = await asyncpg.create_pool(
            database_url,
            min_size=1,
            max_size=10,
            command_timeout=60
        )
    
    async def close(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
    
    async def execute(self, query: str, *args):
        """Execute a query without returning results"""
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)
    
    async def fetch(self, query: str, *args) -> List[Dict]:
        """Execute a query and return all results"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
            return [dict(row) for row in rows]
    
    async def fetchrow(self, query: str, *args) -> Optional[Dict]:
        """Execute a query and return single result"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, *args)
            return dict(row) if row else None
    
    async def fetchval(self, query: str, *args):
        """Execute a query and return single value"""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)


# Global database instance
db = Database()


async def get_court_by_name(court_name: str) -> Optional[Dict]:
    """Get court details by name"""
    query = """
        SELECT c.id, c.name, c.type, d.name as district_name
        FROM courts c
        JOIN districts d ON c.district_id = d.id
        WHERE c.name = $1
    """
    return await db.fetchrow(query, court_name)


async def list_ftsc_courts() -> List[Dict]:
    """List all FTSC courts"""
    query = """
        SELECT c.id, c.name, d.name as district_name
        FROM courts c
        JOIN districts d ON c.district_id = d.id
        WHERE c.type = 'FTSC'
        ORDER BY d.display_order, c.name
    """
    return await db.fetch(query)


async def list_spc_courts() -> List[Dict]:
    """List all SPC courts"""
    query = """
        SELECT c.id, c.name, d.name as district_name
        FROM courts c
        JOIN districts d ON c.district_id = d.id
        WHERE c.type = 'SPC'
        ORDER BY d.display_order, c.name
    """
    return await db.fetch(query)


async def list_all_districts() -> List[Dict]:
    """List all districts"""
    query = """
        SELECT id, name, display_order
        FROM districts
        ORDER BY display_order
    """
    return await db.fetch(query)


async def check_existing_data(court_id: int, month: int, year: int) -> bool:
    """Check if data already exists for court+month+year"""
    query = """
        SELECT EXISTS(
            SELECT 1 FROM court_monthly_data
            WHERE court_id = $1 AND month = $2 AND year = $3
        )
    """
    return await db.fetchval(query, court_id, month, year)


async def get_existing_data(court_id: int, month: int, year: int) -> Optional[Dict]:
    """Get existing monthly data for a court"""
    query = """
        SELECT * FROM court_monthly_data
        WHERE court_id = $1 AND month = $2 AND year = $3
    """
    return await db.fetchrow(query, court_id, month, year)


async def insert_court_monthly_data(court_id: int, month: int, year: int, data: Dict):
    """Insert complete court monthly data"""
    query = """
        INSERT INTO court_monthly_data (
            court_id, month, year,
            balance_rape, balance_pocso, balance_total,
            new_rape, new_pocso, new_total,
            contested_rape, contested_pocso, contested_total,
            disposed_rape, disposed_pocso, disposed_total,
            pending_rape, pending_pocso, pending_total,
            disposal_5y_rape, disposal_5y_pocso, disposal_5y_total,
            pending_over_5y_rape, pending_over_5y_pocso, pending_over_5y_total,
            pending_less_2m_rape, pending_less_2m_pocso,
            pending_2_12m_rape, pending_2_12m_pocso,
            pending_12m_5y_rape, pending_12m_5y_pocso,
            pending_beyond_5y_rape, pending_beyond_5y_pocso,
            total_pendency_rape, total_pendency_pocso,
            disposal_within_2m_rape, disposal_within_2m_pocso,
            disposal_2_12m_rape, disposal_2_12m_pocso,
            disposal_beyond_12m_rape, disposal_beyond_12m_pocso,
            total_disposal_rape, total_disposal_pocso,
            convictions_rape, convictions_pocso
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
            $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
            $23, $24, $25, $26, $27, $28, $29, $30, $31, $32,
            $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44
        )
    """
    await db.execute(
        query,
        court_id, month, year,
        data['balance_rape'], data['balance_pocso'], data['balance_total'],
        data['new_rape'], data['new_pocso'], data['new_total'],
        data['contested_rape'], data['contested_pocso'], data['contested_total'],
        data['disposed_rape'], data['disposed_pocso'], data['disposed_total'],
        data['pending_rape'], data['pending_pocso'], data['pending_total'],
        data['disposal_5y_rape'], data['disposal_5y_pocso'], data['disposal_5y_total'],
        data['pending_over_5y_rape'], data['pending_over_5y_pocso'], data['pending_over_5y_total'],
        data['pending_less_2m_rape'], data['pending_less_2m_pocso'],
        data['pending_2_12m_rape'], data['pending_2_12m_pocso'],
        data['pending_12m_5y_rape'], data['pending_12m_5y_pocso'],
        data['pending_beyond_5y_rape'], data['pending_beyond_5y_pocso'],
        data['total_pendency_rape'], data['total_pendency_pocso'],
        data['disposal_within_2m_rape'], data['disposal_within_2m_pocso'],
        data['disposal_2_12m_rape'], data['disposal_2_12m_pocso'],
        data['disposal_beyond_12m_rape'], data['disposal_beyond_12m_pocso'],
        data['total_disposal_rape'], data['total_disposal_pocso'],
        data['convictions_rape'], data['convictions_pocso']
    )


async def delete_court_monthly_data(court_id: int, month: int, year: int):
    """Delete monthly data for a court"""
    query = """
        DELETE FROM court_monthly_data
        WHERE court_id = $1 AND month = $2 AND year = $3
    """
    await db.execute(query, court_id, month, year)


async def query_ftsc_summary(district_name: Optional[str] = None, 
                            month: Optional[int] = None, 
                            year: Optional[int] = None) -> List[Dict]:
    """Query FTSC district summary view"""
    query = "SELECT * FROM ftsc_district_summary WHERE 1=1"
    params = []
    param_count = 1
    
    if district_name:
        query += f" AND district_name = ${param_count}"
        params.append(district_name)
        param_count += 1
    
    if month:
        query += f" AND month = ${param_count}"
        params.append(month)
        param_count += 1
    
    if year:
        query += f" AND year = ${param_count}"
        params.append(year)
        param_count += 1
    
    query += " ORDER BY display_order, year, month"
    
    return await db.fetch(query, *params)


async def query_spc_data(court_name: Optional[str] = None,
                        district_name: Optional[str] = None,
                        month: Optional[int] = None,
                        year: Optional[int] = None) -> List[Dict]:
    """Query SPC court data view"""
    query = "SELECT * FROM spc_court_data WHERE 1=1"
    params = []
    param_count = 1
    
    if court_name:
        query += f" AND court_name = ${param_count}"
        params.append(court_name)
        param_count += 1
    
    if district_name:
        query += f" AND district_name = ${param_count}"
        params.append(district_name)
        param_count += 1
    
    if month:
        query += f" AND month = ${param_count}"
        params.append(month)
        param_count += 1
    
    if year:
        query += f" AND year = ${param_count}"
        params.append(year)
        param_count += 1
    
    query += " ORDER BY display_order, court_name, year, month"
    
    return await db.fetch(query, *params)
