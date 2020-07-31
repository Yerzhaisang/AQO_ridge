The main part of this code was credited from https://github.com/postgrespro/aqo .(with permission)

```
cd postgresql                                                # enter postgresql source directory
git clone https://github.com/Yerzhaisang/aqo.git contrib/aqo        # clone aqo into contrib
patch -p1 --no-backup-if-mismatch < contrib/aqo/aqo_pg<version>.patch  # patch postgresql
make clean && make && make install                               # recompile postgresql
cd contrib/aqo                                                   # enter aqo directory
make && make install                                             # install aqo
make check                                              # check whether it works correctly (optional)
```

Tag version at the patch name corresponds to suitable PostgreSQL release. For PostgreSQL 10 use aqo_pg10.patch; for PostgreSQL 11 use aqo_pg11.patch and so on. Also, you can see git tags at the master branch for more accurate definition of suitable PostgreSQL version.

In your database:

CREATE EXTENSION aqo;

Modify your postgresql.conf:

shared_preload_libraries = 'aqo'

and restart PostgreSQL.

It is essential that library is preloaded during server startup, because adaptive query optimization must be enabled on per-cluster basis instead of per-database.
