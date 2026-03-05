"""Tests for MySQLIO (config and spec only -- no live database).

All tests use ``driver="pymysql"`` because SQLAlchemy's default MySQL dialect
(``mysqldb``) eagerly imports the ``MySQLdb`` package at engine-creation time,
and that package is not installed in this environment.
"""


from interloper.io.database import WriteDisposition

from interloper_sql import MySQLIO

# Default kwargs used by every test so the engine can be created with pymysql.
_DEFAULTS = {"host": "localhost", "database": "mydb", "driver": "pymysql"}


# ---------------------------------------------------------------------------
# Init / parameter storage
# ---------------------------------------------------------------------------


class TestMySQLIOInit:
    """Constructor stores connection parameters correctly."""

    def test_required_host_and_database(self):
        """host and database are required positional arguments."""
        io = MySQLIO(host="db.example.com", database="mydb", driver="pymysql")
        assert io.host == "db.example.com"
        assert io.database == "mydb"

    def test_default_port(self):
        """Default port is 3306."""
        io = MySQLIO(**_DEFAULTS)
        assert io.port == 3306

    def test_custom_port(self):
        """Custom port is preserved."""
        io = MySQLIO(**_DEFAULTS, port=3307)
        assert io.port == 3307

    def test_default_user(self):
        """Default user is 'root'."""
        io = MySQLIO(**_DEFAULTS)
        assert io.user == "root"

    def test_custom_user(self):
        """Custom user is preserved."""
        io = MySQLIO(**_DEFAULTS, user="admin")
        assert io.user == "admin"

    def test_password_none_by_default(self):
        """Password is None by default."""
        io = MySQLIO(**_DEFAULTS)
        assert io.password is None

    def test_custom_password(self):
        """Custom password is preserved."""
        io = MySQLIO(**_DEFAULTS, password="secret")
        assert io.password == "secret"

    def test_custom_driver(self):
        """Custom driver is preserved."""
        io = MySQLIO(**_DEFAULTS)
        assert io.driver == "pymysql"

    def test_default_write_disposition(self):
        """Default write disposition is REPLACE."""
        io = MySQLIO(**_DEFAULTS)
        assert io.write_disposition is WriteDisposition.REPLACE

    def test_custom_write_disposition(self):
        """Explicit write disposition is preserved."""
        io = MySQLIO(**_DEFAULTS, write_disposition=WriteDisposition.APPEND)
        assert io.write_disposition is WriteDisposition.APPEND

    def test_default_chunk_size(self):
        """Default chunk_size is 1000."""
        io = MySQLIO(**_DEFAULTS)
        assert io.chunk_size == 1000

    def test_custom_chunk_size(self):
        """Explicit chunk_size is preserved."""
        io = MySQLIO(**_DEFAULTS, chunk_size=250)
        assert io.chunk_size == 250


# ---------------------------------------------------------------------------
# to_spec
# ---------------------------------------------------------------------------


class TestMySQLIOSpec:
    """to_spec serialization."""

    def test_to_spec_path(self):
        """Spec path points to MySQLIO."""
        io = MySQLIO(**_DEFAULTS)
        spec = io.to_spec()
        assert spec.path == "interloper_sql.io.mysql.MySQLIO"

    def test_to_spec_captures_connection_params(self):
        """Spec captures host, port, database, user."""
        io = MySQLIO(host="db.example.com", database="mydb", driver="pymysql")
        spec = io.to_spec()

        assert spec.init["host"] == "db.example.com"
        assert spec.init["port"] == 3306
        assert spec.init["database"] == "mydb"
        assert spec.init["user"] == "root"

    def test_to_spec_full(self):
        """Spec with all parameters set."""
        io = MySQLIO(
            host="db.example.com",
            database="mydb",
            port=3307,
            user="admin",
            password="secret",
            driver="pymysql",
            write_disposition=WriteDisposition.APPEND,
            chunk_size=500,
        )
        spec = io.to_spec()

        assert spec.init["host"] == "db.example.com"
        assert spec.init["port"] == 3307
        assert spec.init["database"] == "mydb"
        assert spec.init["user"] == "admin"
        assert spec.init["password"] == "secret"
        assert spec.init["driver"] == "pymysql"
        assert spec.init["write_disposition"] == "append"
        assert spec.init["chunk_size"] == 500

    def test_to_spec_omits_none_password(self):
        """Password is omitted from spec when None."""
        io = MySQLIO(**_DEFAULTS)
        spec = io.to_spec()
        assert "password" not in spec.init

    def test_to_spec_includes_driver(self):
        """Driver is included in spec when set."""
        io = MySQLIO(**_DEFAULTS)
        spec = io.to_spec()
        assert spec.init["driver"] == "pymysql"


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------


class TestMySQLIOUrl:
    """Internal SQLAlchemy URL construction."""

    def test_url_backend_name(self):
        """URL backend name is 'mysql'."""
        io = MySQLIO(**_DEFAULTS, user="me", password="pw")
        url = io._engine.url
        assert url.get_backend_name() == "mysql"

    def test_url_connection_params(self):
        """URL carries host, port, database, and user."""
        io = MySQLIO(host="dbhost", database="testdb", user="me", password="pw", driver="pymysql")
        url = io._engine.url

        assert url.host == "dbhost"
        assert url.port == 3306
        assert url.database == "testdb"
        assert url.username == "me"

    def test_url_with_driver(self):
        """URL uses 'mysql+pymysql' drivername."""
        io = MySQLIO(**_DEFAULTS)
        url = io._engine.url
        assert str(url).startswith("mysql+pymysql://")
