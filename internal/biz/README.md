# Biz
+-------------------+       +-------------------+
|   Memory Cache     | <-->  | Persistence Layer  |
| (map[string]CacheItem)     | (Disk File)       |
+-------------------+       +-------------------+
^                           ^
| Set/Get/Delete           | Save/Load
+---------------------------+