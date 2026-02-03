#!/usr/bin/env node
/**
 * Script to load account data from MoveAccount.csv to customer.esac_account_contacts table
 * Node.js version
 */

const fs = require('fs');
const csv = require('csv-parser');
const { Pool } = require('pg');
const path = require('path');

// Database connection configuration
// Update these values according to your database setup
const dbConfig = {
    host: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT || 5432,
    database: process.env.DB_NAME || 'your_database',
    user: process.env.DB_USER || 'your_username',
    password: process.env.DB_PASSWORD || 'your_password',
    max: 10, // maximum number of clients in the pool
    idleTimeoutMillis: 30000,
};

// Create connection pool
const pool = new Pool(dbConfig);

/**
 * Extract device_kit_id from hex-encoded feature_value
 * Handles multiple feature_values in a single cell
 * Equivalent to: SELECT SPLIT_PART(CONVERT_FROM(DECODE('external_id', 'hex'), 'UTF8'), '.', 2)::INTEGER AS device_kit_id;
 */
function extractDeviceKitIds(featureValue) {
    if (!featureValue || featureValue === '') {
        return [];
    }
    
    // Handle multiple feature_values separated by common delimiters
    const delimiters = [',', ';', '|', '\n', '\t'];
    let values = [featureValue.toString().trim()];
    
    // Split by each delimiter
    for (const delimiter of delimiters) {
        const tempValues = [];
        for (const value of values) {
            tempValues.push(...value.split(delimiter));
        }
        values = tempValues;
    }
    
    const extractedIds = [];
    
    for (const singleValue of values) {
        const trimmedValue = singleValue.trim();
        
        if (!trimmedValue) continue;
        
        // Skip values starting with '+'
        if (trimmedValue.startsWith('+')) {
            console.log(`Skipping feature_value '${trimmedValue}' - starts with '+'`);
            continue;
        }
        
        try {
            // Decode hex to UTF8 string
            const buffer = Buffer.from(trimmedValue, 'hex');
            const decodedString = buffer.toString('utf8');
            
            // Split by '.' and get the second part (index 1)
            const parts = decodedString.split('.');
            if (parts.length > 1) {
                const deviceKitId = parseInt(parts[1]);
                if (!isNaN(deviceKitId)) {
                    extractedIds.push(deviceKitId);
                    console.log(`Extracted device_kit_id ${deviceKitId} from: ${trimmedValue} -> ${decodedString}`);
                }
            }
        } catch (error) {
            console.warn(`Error decoding feature_value ${trimmedValue}: ${error.message}`);
        }
    }
    
    return extractedIds;
}

/**
 * Clean and parse extension_value field
 * Some values appear to be JSON arrays, others are plain strings
 */
function cleanExtensionValue(extVal) {
    if (!extVal || extVal === '') {
        return null;
    }
    
    const cleanedVal = extVal.toString().trim();
    
    // If it's a JSON array string, try to parse and extract first valid value
    if (cleanedVal.startsWith('[') && cleanedVal.endsWith(']')) {
        try {
            const parsed = JSON.parse(cleanedVal);
            if (Array.isArray(parsed) && parsed.length > 0) {
                // Return the first non-empty value
                for (const item of parsed) {
                    if (item && item.toString().trim()) {
                        return item.toString().trim().replace(/^"|"$/g, '');
                    }
                }
            }
        } catch (error) {
            console.warn(`Failed to parse JSON: ${cleanedVal}`);
        }
    }
    
    return cleanedVal;
}

/**
 * Load and preprocess CSV data
 */
function loadCsvData(csvFilePath) {
    return new Promise((resolve, reject) => {
        const records = [];
        
        console.log(`Loading data from ${csvFilePath}`);
        
        fs.createReadStream(csvFilePath)
            .pipe(csv())
            .on('data', (row) => {
                // Direct 1:1 mapping - CSV column names match database column names exactly
                const processedRow = {
                    account_id: parseInt(row.account_id),
                    emails: row.emails || '',
                    phone_numbers: row.phone_numbers || '',
                    extensions: row.extensions || '', // Direct mapping from CSV extensions column
                    created_by: 'By system' // Fixed value as specified
                };
                
                console.log(`Processing account_id ${row.account_id}`);
                records.push(processedRow);
            })
            .on('end', () => {
                console.log(`Loaded ${records.length} records from CSV`);
                resolve(records);
            })
            .on('error', (error) => {
                reject(error);
            });
    });
}

/**
 * Insert data into customer.esac_account_contacts table
 */
async function insertDataToDatabase(records) {
    const client = await pool.connect();
    
    try {
        await client.query('BEGIN');
        
        // Prepare batch insert query
        // Adjust column names to match your actual table schema
        const insertQuery = `
            INSERT INTO customer.esac_account_contacts 
            (account_id, emails, phone_numbers, extensions, created_by)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (account_id) 
            DO UPDATE SET 
                emails = EXCLUDED.emails,
                phone_numbers = EXCLUDED.phone_numbers,
                extensions = EXCLUDED.extensions,
                created_by = EXCLUDED.created_by,
                updated_at = CURRENT_TIMESTAMP
        `;
        
        // Insert records one by one (for better error handling)
        // For better performance with large datasets, consider using batch operations
        let successCount = 0;
        let errorCount = 0;
        
        for (const record of records) {
            try {
                await client.query(insertQuery, [
                    record.account_id,
                    record.emails,
                    record.phone_numbers,
                    record.extensions, // device_kit_id extracted from feature_value
                    record.created_by
                ]);
                successCount++;
            } catch (error) {
                console.error(`Error inserting record ${record.account_id}: ${error.message}`);
                errorCount++;
            }
        }
        
        await client.query('COMMIT');
        console.log(`Successfully processed ${successCount} records (${errorCount} errors)`);
        
    } catch (error) {
        await client.query('ROLLBACK');
        console.error('Error during batch insert:', error);
        throw error;
    } finally {
        client.release();
    }
}

/**
 * Alternative batch insert method for better performance
 */
async function batchInsertDataToDatabase(records) {
    const client = await pool.connect();
    
    try {
        await client.query('BEGIN');
        
        // Prepare batch values
        const values = [];
        const params = [];
        let paramIndex = 1;
        
        for (const record of records) {
            values.push(`($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2}, $${paramIndex + 3}, $${paramIndex + 4})`);
            params.push(
                record.account_id,
                record.emails,
                record.phone_numbers,
                record.extensions, // device_kit_id extracted from feature_value
                record.created_by
            );
            paramIndex += 5;
        }
        
        const batchInsertQuery = `
            INSERT INTO customer.esac_account_contacts 
            (account_id, emails, phone_numbers, extensions, created_by)
            VALUES ${values.join(', ')}
            ON CONFLICT (account_id) 
            DO UPDATE SET 
                emails = EXCLUDED.emails,
                phone_numbers = EXCLUDED.phone_numbers,
                extensions = EXCLUDED.extensions,
                created_by = EXCLUDED.created_by,
                updated_at = CURRENT_TIMESTAMP
        `;
        
        await client.query(batchInsertQuery, params);
        await client.query('COMMIT');
        
        console.log(`Successfully batch inserted ${records.length} records`);
        
    } catch (error) {
        await client.query('ROLLBACK');
        console.error('Error during batch insert:', error);
        throw error;
    } finally {
        client.release();
    }
}

/**
 * Verify that data was inserted correctly
 */
async function verifyDataInsertion() {
    const client = await pool.connect();
    
    try {
        // Get count of records
        const countResult = await client.query('SELECT COUNT(*) FROM customer.esac_account_contacts');
        const totalCount = countResult.rows[0].count;
        
        // Get sample records
        const sampleResult = await client.query(`
            SELECT eac.* FROM customer.esac_account_contacts AS eac 
            ORDER BY account_id 
            LIMIT 5
        `);
        
        console.log(`\nVerification:`);
        console.log(`Total records in table: ${totalCount}`);
        console.log(`Sample records:`);
        sampleResult.rows.forEach(record => {
            console.log(`  ${JSON.stringify(record)}`);
        });
        
    } catch (error) {
        console.error('Error during verification:', error);
    } finally {
        client.release();
    }
}

/**
 * Main execution function
 */
async function main() {
    const csvFilePath = path.join(__dirname, 'MoveAccount.csv');
    
    try {
        // Check if CSV file exists
        if (!fs.existsSync(csvFilePath)) {
            throw new Error(`CSV file not found: ${csvFilePath}`);
        }
        
        // Load CSV data
        const records = await loadCsvData(csvFilePath);
        
        if (records.length === 0) {
            console.log('No records found in CSV file');
            return;
        }
        
        // Choose insertion method based on record count
        if (records.length > 100) {
            console.log('Using batch insert for large dataset...');
            await batchInsertDataToDatabase(records);
        } else {
            console.log('Using individual insert for small dataset...');
            await insertDataToDatabase(records);
        }
        
        // Verify insertion
        await verifyDataInsertion();
        
        console.log('\nData migration completed successfully!');
        
    } catch (error) {
        console.error('Error during migration:', error);
        process.exit(1);
    } finally {
        // Close database pool
        await pool.end();
    }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
    console.log('\nShutting down gracefully...');
    await pool.end();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('\nShutting down gracefully...');
    await pool.end();
    process.exit(0);
});

// Run the script
if (require.main === module) {
    main();
}

module.exports = {
    loadCsvData,
    cleanExtensionValue,
    extractDeviceKitIds,
    insertDataToDatabase,
    verifyDataInsertion
};