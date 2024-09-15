# Clairvoyant
My Senior Project


## .env Vars
| Name | Description |
|------|-----------------|
| PGADMIN_EMAIL | Email for sql editor |
| PGADMIN_PASSWORD | Password for sql editor |
| DB_NAME | Name of db |
| DB_USER | User for db |
| DB_PASSWORD | Password for db |
| DB_HOST | db host |
| DB_PORT | db port |

## Run Program
1. Set up .env with appropriate variables
2. Run:
```sh
docker compose up --build
```
or 
```sh
docker compose up -d
```
3. Go to SQL editor (127.0.0.1/5050), login and create server to see changes.
