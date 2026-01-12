![MIT License](https://img.shields.io/badge/License-MIT-blue.svg)
![Code Coverage](https://codecov.io/gh/pikulo-kama/kama-db/branch/master/graph/badge.svg)
# <img src=".idea/icon.svg" alt="Kama Logo" width="auto" height="100"/> kama-db (kdb)

A lightweight SQLite ORM wrapper for modular Python projects.

## Overview

`kdb` provides a high-level, object-oriented interface for interacting with SQLite databases. It abstracts the complexities of connection management and SQL execution into three primary components:

* **DatabaseManager**: Handles the SQLite connection and low-level execution.
* **DatabaseTable**: Manages table-level operations, filtering, and row collections.
* **DatabaseRow**: Represents individual records with local change tracking.

## Components

### DatabaseManager
The entry point of the library. It manages the file path to the database and provides methods to execute statements and retrieve tables.

### DatabaseTable
Wraps a specific database table. It supports:
- **Fluent API**: Chain `where()` and `order_by()` calls.
- **Data Retrieval**: Syncs database state to local `DatabaseRow` objects.
- **Staged Changes**: Add or remove rows locally before persisting.
- **Primary Key Detection**: Automatically identifies PKs to ensure safe updates.

### DatabaseRow
A data transfer object for table records.
- **Case-Insensitive**: Column lookups are normalized to lowercase.
- **Edit Tracking**: Maintains a map of "edits" separate from original data.
- **State Management**: Tracks whether a record is "new" (unsaved) or existing.

## Usage Example

```python
from kdb.manager import DatabaseManager

# 1. Initialize
db = DatabaseManager("app_data.db")

# 2. Access a table with filters
users = db.table("users")
users.where("active = ?", 1) \
    .order_by("created_at DESC") \
    .retrieve()

# 3. Edit existing data
if not users.is_empty:
    users.set(1, "last_login", "2023-10-27")

# 4. Add new data
new_row_idx = users.add_row()
users.set(new_row_idx, "username", "new_user")
users.set(new_row_idx, "email", "user@example.com")

# 5. Persist changes
# This handles DELETEs, UPDATEs, and INSERTs in one transaction
users.save()
```