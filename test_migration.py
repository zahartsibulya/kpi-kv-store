from coordinator.main import ConsistentHashRing


def run_migration_benchmark(total_keys: int = 10_000):
    keys = [f"user_{i}" for i in range(total_keys)]

    # 1. Consistent Hashing (100 віртуальних вузлів)
    ring_2 = ConsistentHashRing(virtual_nodes=100)
    ring_2.add_shard("shard-1", "http://s1")
    ring_2.add_shard("shard-2", "http://s2")

    ring_3 = ConsistentHashRing(virtual_nodes=100)
    ring_3.add_shard("shard-1", "http://s1")
    ring_3.add_shard("shard-2", "http://s2")
    ring_3.add_shard("shard-3", "http://s3")

    ch_moved = sum(1 for k in keys if ring_2.get_shard(k)[0] != ring_3.get_shard(k)[0])

    # 2. Звичайний Modulo Hashing (hash % N)
    mod_moved = sum(1 for k in keys if (hash(k) % 2) != (hash(k) % 3))

    print(f"Всього протестовано ключів: {total_keys}")
    print("-" * 50)
    print(f"Звичайний (hash % N): переїхало {mod_moved} ключів ({mod_moved / total_keys * 100:.2f}%)")
    print(f"Consistent Hashing:   переїхало {ch_moved} ключів ({ch_moved / total_keys * 100:.2f}%)")
    print(f"Теоретичний ідеал при додаванні 3-го вузла: ~33.33% ({total_keys // 3} ключів)")


if __name__ == "__main__":
    run_migration_benchmark()