-- SQL Script to load data from MoveAccount.csv to customer.esac_account_contacts
-- This assumes you can use COPY command or have imported the CSV to a temporary table

-- Option 1: If you can use COPY command directly
-- First create a temporary table to match CSV structure
CREATE TEMP TABLE temp_move_accounts (
    account_id INTEGER,
    feature_value TEXT,
    emails TEXT,
    phone_numbers TEXT
);

-- Load CSV data into temp table (adjust path as needed)
\COPY temp_move_accounts FROM '/Users/myadav/Documents/move accounts/MoveAccount.csv' WITH CSV HEADER;

-- Clean and insert data into target table
INSERT INTO customer.esac_account_contacts (account_id, extensions, emails, phone_numbers, created_by)
SELECT 
    account_id,
    -- Extract device_kit_id from feature_value (skip if starts with '+')
    CASE 
        WHEN feature_value IS NULL OR feature_value = '' OR feature_value ~ '^\+' THEN
            NULL
        ELSE
            -- Convert hex to UTF8 and extract device_kit_id
            SPLIT_PART(CONVERT_FROM(DECODE(feature_value, 'hex'), 'UTF8'), '.', 2)::INTEGER
    END as extensions,
    emails,
    phone_numbers,
    'By system' as created_by
FROM temp_move_accounts
ON CONFLICT (account_id) 
DO UPDATE SET 
    extensions = EXCLUDED.extensions,
    emails = EXCLUDED.emails,
    phone_numbers = EXCLUDED.phone_numbers,
    created_by = EXCLUDED.created_by,
    updated_at = CURRENT_TIMESTAMP;

-- Verify the data
SELECT COUNT(*) as total_records FROM customer.esac_account_contacts;

-- Show sample of inserted data
SELECT eac.* FROM customer.esac_account_contacts AS eac 
WHERE account_id IN (850527, 850398, 844559)
ORDER BY account_id;

-- Clean up temp table
DROP TABLE temp_move_accounts;

-- Option 2: Alternative approach using VALUES clause for small datasets
-- If you have a small dataset, you can insert directly:

/*
INSERT INTO customer.esac_account_contacts (account_id, extensions, emails, phone_numbers, created_by)
VALUES 
    (850527, 297247088, 'yadav@gmail.com', '18109651265', 'By system'),
    (850398, 233916826, 'mithlesh1@gmail.com', '18109661265', 'By system'),
    (844559, 233916827, 'yadavmithlesh122@gmail.com', '18209661265', 'By system')
ON CONFLICT (account_id) 
DO UPDATE SET 
    extensions = EXCLUDED.extensions,
    emails = EXCLUDED.emails,
    phone_numbers = EXCLUDED.phone_numbers,
    created_by = EXCLUDED.created_by,
    updated_at = CURRENT_TIMESTAMP;
*/