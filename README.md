# public-info

## Running Postgres with Docker

1. **Build and start the Postgres container using Docker Compose:**
   ```sh
   docker-compose up -d
   ```

2. **Stop the container:**
   ```sh
   docker-compose down
   ```

- The database will be available at `localhost:5432` with the credentials specified in `docker-compose.yml`.
- test commit