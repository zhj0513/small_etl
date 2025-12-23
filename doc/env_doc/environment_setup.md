## minio与postgresql已用该docker-compose.yml启动

```
services:
  minio:
    image: docker.io/minio/minio:latest
    container_name: minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin123
    volumes:
      - ./data/minio:/data
    ports:
      - "19000:9000"
      - "19001:9001"
    restart: unless-stopped

  postgres:
    image: docker.io/postgres:15
    container_name: postgres
    environment:
      POSTGRES_USER: etl
      POSTGRES_PASSWORD: etlpass
      POSTGRES_DB: etl_db
    volumes:
      - ./data/pg:/var/lib/postgresql/data
    ports:
      - "15432:5432"
    restart: unless-stopped
```

## minio bucket
fake-data-for-training
