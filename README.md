## Usage

The ETL pipeline is managed through the `etl.py` script, which uses the `fire` library to provide a command-line interface.

### 1. Extract

Fetch the top 50 cryptocurrencies from the CoinGecko API and save the raw data to a JSON file:

```bash
python etl.py extract --save_path=raw.json
```

### 2. Transform

Transform the raw JSON data into a cleaned CSV file:

```bash
python etl.py transform --input_path=raw.json --output_path=crypto_top_50.csv
```

### 3. Load

Load the cleaned data from the CSV file into a PostgreSQL database.

**Note:** This step requires database credentials. See the "Database Configuration" section below.

```bash
python etl.py load --csv_path=crypto_top_50.csv --db_url=<your-db-url>
```

### 4. Visualize

Generate and save charts from the cleaned data:

```bash
python etl.py visualize --csv_path=crypto_top_50.csv
```

This will create two files: `market_share.png` and `price_bar.png`.

### Running the API and Dashboard

To run the FastAPI and Streamlit dashboard, you can use Docker Compose (recommended for a full setup) or run them locally.

#### Using Docker Compose (Recommended)

Ensure your `.env` file is configured with PostgreSQL credentials. Then, simply run:

```bash
docker-compose up --build
```

This will start the PostgreSQL database, the ETL app, the FastAPI, and the Streamlit dashboard. Once all services are up:

*   **FastAPI:** Accessible at `http://localhost:8000` (or `http://your-docker-host-ip:8000`)
*   **Streamlit Dashboard:** Accessible at `http://localhost:8501` (or `http://your-docker-host-ip:8501`)

#### Running Locally (Requires manual setup)

1.  **Start PostgreSQL:** Ensure your PostgreSQL database is running and accessible.
2.  **Start FastAPI:**
    ```bash
    uvicorn api:app --host 0.0.0.0 --port 8000
    ```
3.  **Start Streamlit Dashboard:**
    ```bash
    streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0
    ```

## Database Configuration

To use the `load` command, you need to provide a PostgreSQL database URL. You can do this in two ways:

1.  **As a command-line argument:**

    Pass the `--db_url` argument to the `load` command:

    ```bash
    python etl.py load --csv_path=crypto_top_50.csv --db_url="postgresql+psycopg2://user:password@host:port/dbname"
    ```

2.  **Using a `.env` file:**

    Create a `.env` file in the root of the project and add your database URL to it:

    ```
    DB_URL="postgresql+psycopg2://user:password@host:port/dbname"
    ```

    The `etl.py` script will automatically load the `DB_URL` from the `.env` file.

## Docker & Docker Compose

This project can be run fully containerized using Docker and Docker Compose. This is the recommended way for production or team environments.

### 1. Build and Start the Services

```bash
docker-compose up --build
```

This will start both the PostgreSQL database and the Python ETL app. The app will run the pipeline using the environment variables defined in your `.env` file.

### 2. Environment Variables

Copy the provided `.env` example and update as needed:

```
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=crypto
POSTGRES_HOST=db
POSTGRES_PORT=5432
```

### 3. Stopping the Services

To stop and remove the containers, run:

```bash
docker-compose down
```

### 4. Running ETL Steps Manually in Docker

You can run any ETL step inside the running app container. For example:

```bash
docker-compose run app python etl.py extract --save_path=raw.json
```

## Professional Setup
- All sensitive credentials are managed via `.env` and never hardcoded.
- The pipeline, database, and dependencies are fully containerized for easy deployment and reproducibility.
- See the `docker-compose.yml` and `Dockerfile` for details.

## Scalability and Performance Considerations

To effectively manage a significant increase in data volume and ensure the long-term robustness and scalability of this ETL pipeline, consider the following strategies across your database and ETL processes:

### Database Optimization (PostgreSQL)

*   **Indexing:** Create appropriate indexes on frequently queried columns (`id`, `last_updated`, etc.) to improve query performance. Regularly analyze query performance and optimize indexes as needed.
*   **Partitioning:** For extremely large tables (millions or billions of rows), consider partitioning to divide data into smaller, more manageable pieces. This improves query performance and simplifies data management.
*   **Connection Pooling:** For high-concurrency scenarios, use a connection pool (e.g., PgBouncer) to efficiently manage and reuse database connections, reducing overhead.
*   **Hardware Scaling & Configuration Tuning:** Monitor database server resources (CPU, RAM, I/O) and tune PostgreSQL configuration parameters (`work_mem`, `shared_buffers`, etc.) to optimize performance as data grows.
*   **Regular Maintenance:** Perform routine database maintenance tasks like `VACUUM`, `ANALYZE`, and reindexing to reclaim space and update statistics.

### ETL Process Efficiency (Python Script)

*   **Batch Processing/Chunking:** Implement batch processing for reading, transforming, and loading data in smaller, manageable chunks, especially for large datasets. Utilize the `chunksize` parameter in `pandas.DataFrame.to_sql` for efficient loading.
*   **Error Handling and Retries:** Enhance error handling with retry mechanisms (e.g., exponential backoff) for transient issues like API rate limits or network problems, making the pipeline more resilient.
*   **Efficient Data Structures:** For extremely large datasets that don't fit into memory, consider libraries like Dask or PySpark for out-of-core or distributed computing.
*   **Logging and Monitoring:** Leverage comprehensive logging (already implemented) to monitor pipeline performance and debug issues. Integrate with monitoring tools for tracking execution times, data volumes, and error rates.
*   **Resource Management:** Be mindful of memory and CPU usage. Close database connections promptly and avoid unnecessary data copies.

### Data Retention and Archiving

*   **Data Warehousing:** For analytical purposes, consider moving historical data to a dedicated data warehouse (e.g., another PostgreSQL instance optimized for analytics, or specialized solutions like Snowflake, BigQuery, Redshift).
*   **Archiving to Cheaper Storage:** For rarely accessed data, move it to cost-effective storage solutions (e.g., Amazon S3, Google Cloud Storage).
*   **Data Lifecycle Management:** Define clear policies for data retention, movement between storage tiers, and eventual purging.

## Environment Drift Prevention

Maintaining consistent environments across development, testing, and production is crucial for reliable pipeline operation. This project employs several strategies to minimize environment drift:

*   **Dependency Management (`requirements.txt` and Virtual Environments):**
    *   All Python dependencies are explicitly listed with their exact versions in `requirements.txt`. Always install dependencies using `pip install -r requirements.txt`.
    *   Always use a Python virtual environment (`python -m venv venv`) to isolate project dependencies from system-wide installations.
    *   When adding new packages, update `requirements.txt` by running `pip freeze > requirements.txt` to capture exact versions.

*   **Configuration Management (`.env` and `python-dotenv`):
    *   Environment-specific configurations (e.g., database credentials) are stored in a `.env` file and loaded using `python-dotenv`.
    *   A `.env.example` file is provided as a template, outlining all required environment variables.

*   **Containerization (Docker and Docker Compose):**
    *   The most effective way to prevent system-level drifts is through containerization. Docker containers encapsulate the application and all its dependencies (OS, system libraries, Python environment) into a single, portable unit.
    *   The `docker-compose.yml` and `Dockerfile` define the exact, reproducible environment for the application and its services (like PostgreSQL).
    *   Always deploy the application using Docker containers to ensure identical runtime environments across all stages.

## Testing and Linting

This project includes automated tests and uses a linter to ensure code quality and consistency.

### Running Tests

To run the unit tests, navigate to the project root and execute:

```bash
pytest
```

### Linting and Formatting

To check for code style issues and automatically format the code, use `ruff`:

```bash
ruff check .
ruff format .
```

## Enhancing Pipeline Quality Attributes

To further improve the quality of this ETL pipeline, focusing on clarity, maintainability, reusability, testing, and collaboration, consider the following:

### Clarity

*   **Type Hinting:** Add type hints to function signatures and variable declarations to explicitly define expected data types, improving readability and enabling static analysis.
*   **Consistent Code Style:** Enforce strict adherence to a code style guide (e.g., PEP 8) using linters (like `ruff`) to ensure consistent formatting and reduce cognitive load.
*   **Clearer Error Messages:** Ensure error messages are specific, concise, and actionable, guiding users or developers to the root cause of issues.

### Maintainability

*   **Configuration Validation:** Implement more robust validation for configuration parameters (from `.env` or arguments) to ensure they meet expected formats or values.
*   **Dependency Updates:** Regularly review and update dependencies to benefit from bug fixes, performance improvements, and security patches. Automate this process where possible.
*   **Code Reviews:** Establish a formal code review process to catch potential issues early and ensure adherence to standards.
*   **Automated Testing:** A strong test suite (see "Testing and Linting" section) is fundamental for maintainability, providing confidence when making changes.

### Reusability

*   **Parameterization:** Ensure functions and methods are highly parameterized, allowing flexibility and adaptability to different inputs or scenarios without modification.
*   **Abstracting Data Sources/Sinks:** For future expansion, consider abstracting data source and sink interfaces (e.g., using adapter classes) to easily integrate different APIs or databases.
*   **Command-Line Interface (CLI):** The `fire` library already provides a reusable CLI, allowing easy execution of pipeline components.

### Testing

*   **Unit Tests:** Write unit tests for individual functions (e.g., `fetch_top_50_cryptos`, `transform_crypto_data`, `validate_data`) using a framework like `pytest`. Use mocking for external dependencies.
*   **Integration Tests:** Verify interactions between different components (e.g., `extract` -> `transform`, `transform` -> `load`) using a local test database or controlled API responses.
*   **End-to-End (E2E) Tests:** Test the entire pipeline flow from data extraction to final output (e.g., visualization files, database state).
*   **Data Validation Tests:** Implement more sophisticated data validation rules (e.g., range checks, uniqueness, cross-field validation) beyond basic type/null checks.
*   **Performance Tests:** Measure the performance of each pipeline stage to identify bottlenecks, especially for large data volumes.
*   **Test Data:** Create representative test data (mock API responses, small CSVs) for consistent and reproducible test runs.
*   **CI/CD Integration:** Integrate tests into a Continuous Integration/Continuous Deployment (CI/CD) pipeline for automatic execution on every code change.

### Collaboration

*   **Code Review Process:** Establish a formal code review process for all code changes.
*   **Issue Tracking:** Use an issue tracking system (e.g., GitHub Issues, Jira) to manage tasks, bugs, and feature requests.
*   **Contribution Guidelines:** Create a `CONTRIBUTING.md` file outlining how new contributors can get started, coding standards, and the pull request process.
*   **Communication Channels:** Establish clear communication channels (e.g., Slack, Microsoft Teams) for team discussions.
*   **Automated Linters/Formatters:** Enforce code style automatically using tools like `ruff` or `black` integrated into pre-commit hooks or CI/CD pipelines to reduce style-related conflicts.#   c r y p t o - e t l - p i p e l i n e  
 