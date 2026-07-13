from migrate_to_railway import reset_sequences


class RecordingConnection:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(str(statement))


class BeginContext:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        return False


class RecordingEngine:
    def __init__(self):
        self.connection = RecordingConnection()

    def begin(self):
        return BeginContext(self.connection)


def test_reset_sequences_repairs_every_serial_table():
    engine = RecordingEngine()

    reset_sequences(engine)

    sql = "\n".join(engine.connection.statements)
    for table in ["stocks", "daily_prices", "indicators", "ingestion_runs"]:
        assert f"pg_get_serial_sequence('{table}', 'id')" in sql
        assert f"FROM {table}" in sql
