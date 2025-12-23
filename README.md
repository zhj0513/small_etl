# 数据库迁移
pixi run alembic revision --autogenerate -m "Add foreign key constraint for trade.account_id"
pixi run alembic upgrade head
