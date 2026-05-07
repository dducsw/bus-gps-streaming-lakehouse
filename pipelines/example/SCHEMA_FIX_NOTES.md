# Schema Mismatch Fixes Required

## Issue
The scripts expect `route_var_id` column, but the actual CSV files don't have it:

### vehicle_route_mapping.csv columns:
- vehicle
- route_id  
- route_no

### hcmc_stops_agu_2025.csv columns:
- stopId
- code
- stop_name
- type
- zone
- ward
- address
- street
- support_disability
- status
- lng
- lat
- search
- routes (comma-separated route numbers)

## Solution Applied
1. **Method 1**: Fixed to use GPS coordinate progression instead of route variation matching
2. **Method 2**: Needs update to remove route_var_id references
3. **Method 3**: Needs update to remove route_var_id references
4. **Hybrid**: Needs update to all three methods

## Key Changes
- Remove `route_var_id` from all groupBy and join clauses
- Use `route_id` and `route_no` for grouping instead
- Simplify join logic to work with actual CSV columns

## Status
- ✅ Method 1: FIXED - Uses GPS coordinate progression
- ⏳ Method 2: Needs `route_var_id` removal from lines 153, 163, 171, 197
- ⏳ Method 3: Needs `route_var_id` removal
- ⏳ Hybrid: Needs updates to all three embedded methods
