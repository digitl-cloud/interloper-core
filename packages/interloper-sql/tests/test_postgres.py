"""Tests for PostgresIO (config and spec only -- no live database)."""

from interloper.io.database import WriteDisposition

from interloper_sql import PostgresIO

# ---------------------------------------------------------------------------
# Init / parameter storage
# ---------------------------------------------------------------------------


class TestPostgresIOInit:
    """Constructor stores connection parameters correctly."""

    def test_required_host(self):
        """host is a required positional argument."""
        io = PostgresIO(host="db.example.com")
        assert io.host == "db.example.com"

    def test_default_port(self):
        """Default port is 5432."""
        io = PostgresIO(host="localhost")
        assert io.port == 5432

    def test_custom_port(self):
        """Custom port is preserved."""
        io = PostgresIO(host="localhost", port=6543)
        assert io.port == 6543

    def test_default_database(self):
        """Default database is 'postgres'."""
        io = PostgresIO(host="localhost")
        assert io.database == "postgres"

    def test_custom_database(self):
        """Custom database is preserved."""
        io = PostgresIO(host="localhost", database="mydb")
        assert io.database == "mydb"

    def test_default_user(self):
        """Default user is 'postgres'."""
        io = PostgresIO(host="localhost")
        assert io.user == "postgres"

    def test_custom_user(self):
        """Custom user is preserved."""
        io = PostgresIO(host="localhost", user="admin")
        assert io.user == "admin"

    def test_password_none_by_default(self):
        """Password is None by default."""
        io = PostgresIO(host="localhost")
        assert io.password is None

    def test_custom_password(self):
        """Custom password is preserved."""
        io = PostgresIO(host="localhost", password="secret")
        assert io.password == "secret"

    def test_driver_none_by_default(self):
        """Driver is None by default."""
        io = PostgresIO(host="localhost")
        assert io.driver is None

    def test_custom_driver(self):
        """Custom driver is preserved."""
        io = PostgresIO(host="localhost", driver="psycopg2")
        assert io.driver == "psycopg2"

    def test_default_write_disposition(self):
        """Default write disposition is REPLACE."""
        io = PostgresIO(host="localhost")
        assert io.write_disposition is WriteDisposition.REPLACE

    def test_custom_write_disposition(self):
        """Explicit write disposition is preserved."""
        io = PostgresIO(host="localhost", write_disposition=WriteDisposition.APPEND)
        assert io.write_disposition is WriteDisposition.APPEND

    def test_default_chunk_size(self):
        """Default chunk_size is 1000."""
        io = PostgresIO(host="localhost")
        assert io.chunk_size == 1000


# ---------------------------------------------------------------------------
# to_spec
# ---------------------------------------------------------------------------


class TestPostgresIOSpec:
    """to_spec serialization."""

    def test_to_spec_minimal(self):
        """Spec with only required parameters."""
        io = PostgresIO(host="db.example.com")
        spec = io.to_spec()

        assert spec.path == "interloper_sql.io.postgres.PostgresIO"
        assert spec.init["host"] == "db.example.com"
        assert spec.init["port"] == 5432
        assert spec.init["database"] == "postgres"
        assert spec.init["user"] == "postgres"
        assert "password" not in spec.init
        assert "driver" not in spec.init

    def test_to_spec_full(self):
        """Spec with all parameters set."""
        io = PostgresIO(
            host="db.example.com",
            port=6543,
            database="mydb",
            user="admin",
            password="secret",
            driver="psycopg2",
            write_disposition=WriteDisposition.APPEND,
            chunk_size=500,
        )
        spec = io.to_spec()

        assert spec.init["host"] == "db.example.com"
        assert spec.init["port"] == 6543
        assert spec.init["database"] == "mydb"
        assert spec.init["user"] == "admin"
        assert spec.init["password"] == "secret"
        assert spec.init["driver"] == "psycopg2"
        assert spec.init["write_disposition"] == "append"
        assert spec.init["chunk_size"] == 500

    def test_to_spec_omits_none_password(self):
        """Password is omitted from spec when None."""
        io = PostgresIO(host="localhost")
        spec = io.to_spec()
        assert "password" not in spec.init

    def test_to_spec_omits_none_driver(self):
        """Driver is omitted from spec when None."""
        io = PostgresIO(host="localhost")
        spec = io.to_spec()
        assert "driver" not in spec.init


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------


class TestPostgresIOUrl:
    """Internal SQLAlchemy URL construction."""

    def test_url_without_driver(self):
        """URL uses 'postgresql' drivername when no driver specified."""
        io = PostgresIO(host="localhost", database="testdb", user="me", password="pw")
        url = io._engine.url

        assert url.get_backend_name() == "postgresql"
        assert url.host == "localhost"
        assert url.port == 5432
        assert url.database == "testdb"
        assert url.username == "me"

    def test_url_with_driver(self):
        """URL uses 'postgresql+driver' drivername when driver specified."""
        io = PostgresIO(host="localhost", driver="psycopg2")
        url = io._engine.url

        assert str(url).startswith("postgresql+psycopg2://")
