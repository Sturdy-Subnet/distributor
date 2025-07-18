import aiosqlite


def get_db_connection(db_path: str) -> aiosqlite.Connection:
    """
    Get a connection to the SQLite database.

    Args:
        db_path (str): The path to the SQLite database file.

    Returns:
        aiosqlite.Connection: An asynchronous connection to the database.
    """
    return aiosqlite.connect(db_path)
