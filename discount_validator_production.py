# Author: Khushboo kaushik
import logging
import sqlite3
import sys
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime
from typing import Tuple, Final

# =============================================================================
# 0. INFRASTRUCTURE & LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("PrincipalDataEngineer")

# =============================================================================
# 1. MOCKED PRODUCTION DATASETS
# =============================================================================

def get_production_mock_data() -> Tuple[StringIO, StringIO]:
    """
    Generates a high-entropy dataset with complex edge cases:
    - ORD_001: Conflict between two valid exclusive coupons (SAVE10 vs SUMMER20).
    - ORD_003: Expired coupon.
    - ORD_005: Malformed (negative price) -> Should hit DLQ.
    - ORD_007: Duplicate applications of same coupon.
    """
    applied_coupons = """order_id,product_id,product_category,original_price,coupon_code,order_date
ORD_001,P_EL_01,Electronics,1000.0,SAVE10,2026-04-15 10:00:00
ORD_001,P_EL_01,Electronics,1000.0,SUMMER20,2026-04-15 10:00:00
ORD_002,P_CL_02,Clothing,100.0,CLOTH5,2026-04-10 12:00:00
ORD_003,P_HM_03,Home,200.0,EXPIRED10,2026-04-18 09:00:00
ORD_004,P_EL_04,Electronics,500.0,WRONG_CAT,2026-04-12 15:30:00
ORD_005,P_EL_05,Electronics,-50.0,SAVE10,2026-04-15 11:00:00
ORD_006,P_EL_06,Electronics,0.0,SAVE10,2026-04-15 11:00:00
ORD_007,P_HM_07,Home,300.0,DUPE1,2026-04-15 11:00:00
ORD_007,P_HM_07,Home,300.0,DUPE1,2026-04-15 11:00:00
ORD_008,P_BT_08,Beauty,150.0,MAX_CAP,2026-04-15 11:00:00
ORD_009,P_HM_09,Home,,SAVE10,2026-04-15 11:00:00
"""
    coupon_rules = """coupon_code,expiry_date,applicable_scope,discount_type,discount_value,max_cap,is_exclusive
SAVE10,2026-12-31 23:59:59,Electronics,percentage,10.0,50.0,1
SUMMER20,2026-08-31 23:59:59,Electronics,percentage,20.0,150.0,1
CLOTH5,2026-05-31 23:59:59,Clothing,fixed,5.0,,1
EXPIRED10,2026-03-31 23:59:59,Home,percentage,10.0,20.0,1
WRONG_CAT,2026-12-31 23:59:59,Books,percentage,15.0,30.0,1
DUPE1,2026-12-31 23:59:59,Home,fixed,10.0,,1
MAX_CAP,2026-12-31 23:59:59,Beauty,percentage,50.0,20.0,1
"""
    return StringIO(applied_coupons), StringIO(coupon_rules)

# =============================================================================
# 2. ETL PIPELINE ENGINE
# =============================================================================

class DiscountValidatorPipeline:
    """
    Principal-level ETL Pipeline.
    Complexity: O(N log N) due to vectorized sorting for conflict resolution.
    """
    
    DB_NAME: Final[str] = "discount_analytics.db"

    def __init__(self):
        self.df_applied: pd.DataFrame = pd.DataFrame()
        self.df_rules: pd.DataFrame = pd.DataFrame()
        self.df_final: pd.DataFrame = pd.DataFrame()
        self.df_quarantine: pd.DataFrame = pd.DataFrame()

    def extract_and_dlq(self, coupons_io: StringIO, rules_io: StringIO) -> None:
        """Phase 1: Ingest and Quarantine malformed records to DLQ."""
        logger.info("Extracting data and validating structural integrity...")
        
        # Ingest with strict type casting
        raw_coupons = pd.read_csv(coupons_io)
        raw_coupons['original_price'] = pd.to_numeric(raw_coupons['original_price'], errors='coerce')
        raw_coupons['order_date'] = pd.to_datetime(raw_coupons['order_date'], errors='coerce')
        
        # Define Malformed: NaN Price, Negative Price, or Invalid Date
        dlq_mask = (
            raw_coupons['original_price'].isna() | 
            (raw_coupons['original_price'] < 0) | 
            raw_coupons['order_date'].isna()
        )
        
        self.df_quarantine = raw_coupons[dlq_mask].copy()
        self.df_applied = raw_coupons[~dlq_mask].copy()
        
        # Load rules
        self.df_rules = pd.read_csv(rules_io)
        self.df_rules['expiry_date'] = pd.to_datetime(self.df_rules['expiry_date'])
        
        if not self.df_quarantine.empty:
            logger.warning(f"DLQ Alert: {len(self.df_quarantine)} malformed records quarantined.")
            self._write_to_db(self.df_quarantine, "quarantine_logs")

    def transform_rules_engine(self) -> None:
        """Phase 2: Vectorized Rules Engine with Exclusivity Resolution."""
        logger.info("Executing Vectorized Rules Engine...")
        
        # Join data O(N)
        df = self.df_applied.merge(self.df_rules, on='coupon_code', how='left')
        df['rejection_reason'] = None
        df['rejection_reason'] = df['rejection_reason'].astype(object)
        
        # 1. Expiry Check
        df.loc[df['order_date'] > df['expiry_date'], 'rejection_reason'] = 'ERR_EXPIRED'
        
        # 2. Scope/Category Check
        scope_mask = ~((df['applicable_scope'] == df['product_category']) | (df['applicable_scope'] == df['product_id']))
        df.loc[scope_mask & df['rejection_reason'].isna(), 'rejection_reason'] = 'ERR_CAT_MISMATCH'
        
        # 3. Deduplication Check
        dupe_mask = df.duplicated(subset=['order_id', 'coupon_code'], keep='first')
        df.loc[dupe_mask & df['rejection_reason'].isna(), 'rejection_reason'] = 'ERR_DUPE'
        
        # 4. Exclusivity Conflict Resolution (The Principal's Solution)
        # Calculate potential discount for all valid candidates for tie-breaking
        valid_mask = df['rejection_reason'].isna()
        df['potential_disc'] = 0.0
        
        # Vectorized Math for potential discount
        is_pct = df['discount_type'] == 'percentage'
        is_fix = df['discount_type'] == 'fixed'
        
        df.loc[valid_mask & is_pct, 'potential_disc'] = np.minimum(
            df['original_price'] * (df['discount_value'] / 100.0), 
            df['max_cap'].fillna(np.inf)
        )
        df.loc[valid_mask & is_fix, 'potential_disc'] = df['discount_value']

        # Sort by Order and Discount (Descending) to identify the "winner"
        df = df.sort_values(['order_id', 'potential_disc'], ascending=[True, False])
        
        # Logic: If order has an exclusive coupon, pick the top one, reject others
        # We find instances where multiple valid coupons exist for one order_id
        df_valid = df[df['rejection_reason'].isna()].copy()
        conflict_mask = df_valid.groupby('order_id').cumcount() > 0
        
        conflict_indices = df_valid[conflict_mask].index
        df.loc[conflict_indices, 'rejection_reason'] = 'ERR_CONFLICT'
        
        self.df_final = df

    def transform_financials(self) -> None:
        """Phase 3: Finalize Financials with Floor Constraints."""
        df = self.df_final
        df['is_accepted'] = df['rejection_reason'].isna()
        
        # Zero out discounts for rejected coupons
        df['discount_amount'] = np.where(df['is_accepted'], df['potential_disc'], 0.0)
        
        # Final Payable Calculation with Floor at 0.0
        df['final_payable_amount'] = (df['original_price'] - df['discount_amount']).clip(lower=0.0)
        
        self.df_final = df

    def load_acid(self) -> None:
        """Phase 4: Loading to SQLite using Context Managers (Automatic ACID)."""
        logger.info(f"Loading {len(self.df_final)} records to discount_audit...")
        # Prepared for SQL
        out_df = self.df_final.drop(columns=['potential_disc']).copy()
        for col in ['order_date', 'expiry_date']:
            out_df[col] = out_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            
        self._write_to_db(out_df, "discount_audit")
        logger.info("Database load complete.")

    def _write_to_db(self, df: pd.DataFrame, table_name: str) -> None:
        """Private helper using context managers for fail-safe writes."""
        with sqlite3.connect(self.DB_NAME) as conn:
            df.to_sql(table_name, conn, if_exists='append', index=False)

    def generate_report(self) -> None:
        """Phase 5: Analytics & Business Impact Report."""
        df = self.df_final
        accepted = df[df['is_accepted']]
        
        # Financials
        actual_rev = accepted.groupby('order_id')['final_payable_amount'].first().sum()
        # Group by order_id to get unique original prices for counterfactual
        base_rev = df.groupby('order_id')['original_price'].first().sum()
        
        # Loss Prevention (Potential discount of valid-but-rejected/invalid coupons)
        # Specifically: What was the value of the coupons we blocked?
        blocked_value = df[~df['is_accepted'] & df['discount_type'].notna()]['potential_disc'].sum()

        print("\n" + "═"*60)
        print("          DISCOUNT VALIDATOR: BUSINESS IMPACT REPORT")
        print("═"*60)
        print(f"PIPELINE VOLUME:")
        print(f"  Processed: {len(df)} | Accepted: {df['is_accepted'].sum()} | Rejected: {(~df['is_accepted']).sum()}")
        print(f"  Quarantined (DLQ): {len(self.df_quarantine)}")
        
        print(f"\nFINANCIAL METRICS:")
        print(f"  Actual Revenue:         ${actual_rev:,.2f}")
        print(f"  Counterfactual Revenue: ${base_rev:,.2f}")
        print(f"  Total Discount Burn:    ${(base_rev - actual_rev):,.2f}")
        
        print(f"\nLOSS PREVENTION:")
        print(f"  Revenue Protected:      ${blocked_value:,.2f}")
        
        print(f"\nTOP REJECTION REASONS:")
        print(df['rejection_reason'].value_counts().head(3).to_string())
        print("═"*60 + "\n")

# =============================================================================
# 3. MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    # Initialize high-volume pipeline
    etl = DiscountValidatorPipeline()
    
    # 1. Mock Data Streams
    app_io, rules_io = get_production_mock_data()
    
    try:
        # 2. Execute ETL Phases
        etl.extract_and_dlq(app_io, rules_io)
        etl.transform_rules_engine()
        etl.transform_financials()
        etl.load_acid()
        
        # 3. Deliver Insight
        etl.generate_report()
        
        logger.info("Pipeline Status: SUCCESS")
    except Exception as e:
        logger.error(f"Pipeline Status: CRITICAL FAILURE | {str(e)}")
        sys.exit(1)
