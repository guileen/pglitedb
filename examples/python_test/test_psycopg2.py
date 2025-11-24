import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_READ_COMMITTED

def main():
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        user="admin",
        password="password",
        database="testdb"
    )
    
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    
    cur.execute("DROP TABLE IF EXISTS test_users")
    cur.execute("""
        CREATE TABLE test_users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            age INT
        )
    """)
    print("✅ Created table test_users")
    
    cur.execute(
        "INSERT INTO test_users (name, email, age) VALUES (%s, %s, %s) RETURNING id",
        ("Bob", "bob@example.com", 25)
    )
    user_id = cur.fetchone()[0]
    print(f"✅ Inserted user: ID={user_id}")
    
    cur.execute("SELECT id, name, email, age FROM test_users WHERE email = %s", ("bob@example.com",))
    row = cur.fetchone()
    print(f"✅ Selected user: ID={row[0]}, Name={row[1]}, Age={row[3]}")
    
    cur.execute("UPDATE test_users SET age = %s WHERE id = %s", (26, user_id))
    print("✅ Updated user age to 26")
    
    cur.execute("SELECT age FROM test_users WHERE id = %s", (user_id,))
    age = cur.fetchone()[0]
    print(f"✅ Verified update: Age={age}")
    
    conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
    cur.execute("BEGIN")
    cur.execute("UPDATE test_users SET age = %s WHERE id = %s", (27, user_id))
    cur.execute("COMMIT")
    print("✅ Transaction committed")
    
    cur.execute("SELECT age FROM test_users WHERE id = %s", (user_id,))
    age = cur.fetchone()[0]
    assert age == 27
    print(f"✅ Verified transaction: Age={age}")
    
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur.execute("DELETE FROM test_users WHERE id = %s", (user_id,))
    print("✅ Deleted user")
    
    cur.execute("SELECT COUNT(*) FROM test_users")
    count = cur.fetchone()[0]
    print(f"✅ Final user count: {count}")
    
    cur.close()
    conn.close()
    
    print("\n🎉 All psycopg2 tests passed!")

if __name__ == "__main__":
    main()
