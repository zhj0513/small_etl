"""Alembic environment configuration for Small ETL."""

import os
from logging.config import fileConfig

from alembic import context
from hydra import compose, initialize_config_dir
from omegaconf import OmegaConf
from sqlalchemy import engine_from_config, pool
from sqlmodel import SQLModel

from small_etl.domain.models import Asset, Trade  # noqa: F401

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = SQLModel.metadata


def get_url() -> str:
    """Get database URL from Hydra config or environment."""
    if url := os.getenv("DATABASE_URL"):
        return url

    # 获取项目根目录下的 configs 路径
    config_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "configs")
    env = os.getenv("ETL_ENV", "dev")

    with initialize_config_dir(config_dir=config_dir, version_base=None):
        cfg = compose(config_name="config", overrides=[f"db={env}"])
        # 解析插值表达式
        resolved = OmegaConf.to_container(cfg, resolve=True)
        return resolved["db"]["url"]  # type: ignore[return-value]


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
