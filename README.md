# 1) Go to the workspace folder
cd "/Users/myadav/Documents/move accounts"

# 2) Initialize a minimal Node project (creates package.json)
npm init -y

# 3) Install required dependencies
npm install pg csv-parser

# 4) Set database connection environment variables (adjust values)
export DB_HOST="localhost"
export DB_PORT="5432"
export DB_NAME="your_database"
export DB_USER="your_username"
export DB_PASSWORD="your_password"

# 5) Run the Node script
node load_accounts_to_database.js
